package pmtiles

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/phuslu/lru"
	"github.com/rs/cors"
)

type cacheKey struct {
	name   string
	etag   string
	offset uint64 // is 0 for header
	length uint64 // is 0 for header
}

type cachedValue struct {
	header    HeaderV3
	directory []EntryV3
	etag      string
}

// Server is an HTTP server for tiles and metadata.
type Server struct {
	bucket    Bucket
	logger    *log.Logger
	publicURL string
	metrics   *metrics
	cache     *lru.LRUCache[cacheKey, cachedValue]
}

// NewServer creates a new pmtiles HTTP server.
func NewServer(bucketURL string, prefix string, logger *log.Logger, cacheItems int, publicURL string) (*Server, error) {
	ctx := context.Background()

	bucketURL, _, err := NormalizeBucketKey(bucketURL, prefix, "")

	if err != nil {
		return nil, err
	}

	bucket, err := OpenBucket(ctx, bucketURL, prefix)

	if err != nil {
		return nil, err
	}

	return NewServerWithBucket(bucket, prefix, logger, cacheItems, publicURL)
}

// NewServerWithBucket creates a new HTTP server for a gocloud Bucket.
func NewServerWithBucket(bucket Bucket, _ string, logger *log.Logger, cacheItems int, publicURL string) (*Server, error) {
	if cacheItems <= 0 {
		cacheItems = 1024 // A default number of items
	}
	cache := lru.NewLRUCache[cacheKey, cachedValue](cacheItems)

	l := &Server{
		bucket:    bucket,
		logger:    logger,
		publicURL: publicURL,
		metrics:   createMetrics("", logger),
		cache:     cache,
	}
	l.metrics.initCacheStats(cacheItems)

	return l, nil
}

// Start the server HTTP listener.
func (server *Server) Start() {
	// This is now a no-op because the cache is self-managing via GetOrLoad.
}

// headerLoader fetches and parses the header, and also primes the root directory cache.
func (server *Server) headerLoader(ctx context.Context, key cacheKey) (cachedValue, error) {
	var result cachedValue
	offset := int64(0)
	length := int64(16384)

	status := ""
	tracker := server.metrics.startBucketRequest(key.name, "root")
	defer func() { tracker.finish(ctx, status) }()

	server.logger.Printf("fetching %s %d-%d", key.name, offset, length)
	r, etag, statusCode, err := server.bucket.NewRangeReaderEtag(ctx, key.name+".pmtiles", offset, length, key.etag)
	status = strconv.Itoa(statusCode)
	if err != nil {
		server.logger.Printf("failed to fetch %s %d-%d, %v", key.name, offset, length, err)
		return result, err
	}
	defer r.Close()

	b, err := io.ReadAll(r)
	if err != nil {
		status = "error"
		server.logger.Printf("failed to read body for %s %d-%d, %v", key.name, offset, length, err)
		return result, err
	}

	header, err := DeserializeHeader(b[0:HeaderV3LenBytes])
	if err != nil {
		status = "error"
		server.logger.Printf("parsing header failed: %v", err)
		return result, err
	}

	// populate the root directory into the cache before returning the header
	rootEntries := DeserializeEntries(bytes.NewBuffer(b[header.RootOffset:header.RootOffset+header.RootLength]), header.InternalCompression)
	rootDirectoryValue := cachedValue{directory: rootEntries, etag: etag}
	rootDirectoryKey := cacheKey{name: key.name, offset: header.RootOffset, length: header.RootLength, etag: etag}
	server.cache.Set(rootDirectoryKey, rootDirectoryValue)

	result = cachedValue{header: header, etag: etag}
	return result, nil
}

func (server *Server) purge(name string) {
	server.metrics.reloadFile(name)
	server.logger.Printf("re-fetching directories for changed file %s", name)

	keys := server.cache.AppendKeys(nil)
	for _, k := range keys {
		if k.name == name {
			server.cache.Delete(k)
		}
	}
}

func (server *Server) getHeaderMetadata(ctx context.Context, name string) (bool, HeaderV3, []byte, error) {
	found, header, metadataBytes, err := server.getHeaderMetadataAttempt(ctx, name)

	var refreshErr *RefreshRequiredError
	if errors.As(err, &refreshErr) {
		server.purge(name)
		found, header, metadataBytes, err = server.getHeaderMetadataAttempt(ctx, name)
	}
	return found, header, metadataBytes, err
}

func (server *Server) getHeaderMetadataAttempt(ctx context.Context, name string) (bool, HeaderV3, []byte, error) {
	rootKey := cacheKey{name: name, offset: 0, length: 0}
	if _, ok := server.cache.Peek(rootKey); ok {
		server.metrics.cacheRequest(name, "root", "hit")
	} else {
		server.metrics.cacheRequest(name, "root", "miss")
	}

	rootValue, err, _ := server.cache.GetOrLoad(ctx, rootKey, server.headerLoader)
	if err != nil {
		return false, HeaderV3{}, nil, err
	}
	header := rootValue.header

	status := ""
	tracker := server.metrics.startBucketRequest(name, "metadata")
	defer func() { tracker.finish(ctx, status) }()
	r, _, statusCode, err := server.bucket.NewRangeReaderEtag(ctx, name+".pmtiles", int64(header.MetadataOffset), int64(header.MetadataLength), rootValue.etag)
	status = strconv.Itoa(statusCode)

	if err != nil {
		return false, HeaderV3{}, nil, err
	}
	defer r.Close()

	metadataBytes, err := DeserializeMetadataBytes(r, header.InternalCompression)
	if err != nil {
		status = "error"
		return true, HeaderV3{}, nil, errors.New("unknown compression")
	}

	return true, header, metadataBytes, nil
}

func (server *Server) getTileJSON(ctx context.Context, httpHeaders map[string]string, name string) (int, map[string]string, []byte) {
	found, header, metadataBytes, err := server.getHeaderMetadata(ctx, name)

	if err != nil {
		return 500, httpHeaders, []byte("I/O Error")
	}

	if !found {
		return 404, httpHeaders, []byte("Archive not found")
	}

	var metadataMap map[string]interface{}
	json.Unmarshal(metadataBytes, &metadataMap)

	if server.publicURL == "" {
		return 501, httpHeaders, []byte("PUBLIC_URL must be set for TileJSON")
	}

	tilejsonBytes, err := CreateTileJSON(header, metadataBytes, server.publicURL+"/"+name)
	if err != nil {
		return 500, httpHeaders, []byte("Error generating tilejson")
	}

	httpHeaders["Content-Type"] = "application/json"
	httpHeaders["ETag"] = generateEtag(tilejsonBytes)

	return 200, httpHeaders, tilejsonBytes
}

func (server *Server) getMetadata(ctx context.Context, httpHeaders map[string]string, name string) (int, map[string]string, []byte) {
	found, _, metadataBytes, err := server.getHeaderMetadata(ctx, name)

	if err != nil {
		return 500, httpHeaders, []byte("I/O Error")
	}

	if !found {
		return 404, httpHeaders, []byte("Archive not found")
	}

	httpHeaders["Content-Type"] = "application/json"
	httpHeaders["ETag"] = generateEtag(metadataBytes)
	return 200, httpHeaders, metadataBytes
}
func (server *Server) getTile(ctx context.Context, httpHeaders map[string]string, name string, z uint8, x uint32, y uint32, ext string) (int, map[string]string, []byte) {
	status, headers, data, err := server.getTileAttempt(ctx, httpHeaders, name, z, x, y, ext)
	var refreshErr *RefreshRequiredError
	if errors.As(err, &refreshErr) {
		server.purge(name)
		status, headers, data, _ = server.getTileAttempt(ctx, httpHeaders, name, z, x, y, ext)
	}
	return status, headers, data
}

func (server *Server) getTileAttempt(ctx context.Context, httpHeaders map[string]string, name string, z uint8, x uint32, y uint32, ext string) (int, map[string]string, []byte, error) {
	rootKey := cacheKey{name: name, offset: 0, length: 0}
	if _, ok := server.cache.Peek(rootKey); ok {
		server.metrics.cacheRequest(name, "root", "hit")
	} else {
		server.metrics.cacheRequest(name, "root", "miss")
	}
	rootValue, err, _ := server.cache.GetOrLoad(ctx, rootKey, server.headerLoader)
	if err != nil {
		return 404, httpHeaders, []byte("Archive not found"), err
	}
	header := rootValue.header
	etag := rootValue.etag

	if z < header.MinZoom || z > header.MaxZoom {
		return 404, httpHeaders, []byte("Tile not found"), nil
	}

	switch header.TileType {
	case Mvt:
		if ext != "mvt" {
			return 400, httpHeaders, []byte("path mismatch: archive is type MVT (.mvt)"), nil
		}
	case Png:
		if ext != "png" {
			return 400, httpHeaders, []byte("path mismatch: archive is type PNG (.png)"), nil
		}
	case Jpeg:
		if ext != "jpg" {
			return 400, httpHeaders, []byte("path mismatch: archive is type JPEG (.jpg)"), nil
		}
	case Webp:
		if ext != "webp" {
			return 400, httpHeaders, []byte("path mismatch: archive is type WebP (.webp)"), nil
		}
	case Avif:
		if ext != "avif" {
			return 400, httpHeaders, []byte("path mismatch: archive is type AVIF (.avif)"), nil
		}
	}

	tileID := ZxyToID(z, x, y)
	dirOffset, dirLen := header.RootOffset, header.RootLength

	directoryLoader := func(ctx context.Context, key cacheKey) (cachedValue, error) {
		var result cachedValue
		status := ""
		tracker := server.metrics.startBucketRequest(key.name, "leaf")
		defer func() { tracker.finish(ctx, status) }()

		r, _, statusCode, err := server.bucket.NewRangeReaderEtag(ctx, key.name+".pmtiles", int64(key.offset), int64(key.length), key.etag)
		status = strconv.Itoa(statusCode)
		if err != nil {
			return result, err
		}
		defer r.Close()

		b, err := io.ReadAll(r)
		if err != nil {
			status = "error"
			return result, err
		}

		directory := DeserializeEntries(bytes.NewBuffer(b), header.InternalCompression)
		result = cachedValue{directory: directory, etag: key.etag}
		return result, nil
	}

	for depth := 0; depth <= 3; depth++ {
		dirKey := cacheKey{name: name, offset: dirOffset, length: dirLen, etag: etag}
		if _, ok := server.cache.Peek(dirKey); ok {
			server.metrics.cacheRequest(name, "leaf", "hit")
		} else {
			server.metrics.cacheRequest(name, "leaf", "miss")
		}

		dirValue, err, _ := server.cache.GetOrLoad(ctx, dirKey, directoryLoader)
		if err != nil {
			return 500, httpHeaders, []byte("I/O Error"), err
		}

		directory := dirValue.directory
		entry, ok := FindTile(directory, tileID)
		if !ok {
			break
		}

		if entry.RunLength > 0 {
			status := ""
			tracker := server.metrics.startBucketRequest(name, "tile")
			defer func() { tracker.finish(ctx, status) }()
			r, _, statusCode, err := server.bucket.NewRangeReaderEtag(ctx, name+".pmtiles", int64(header.TileDataOffset+entry.Offset), int64(entry.Length), etag)
			status = strconv.Itoa(statusCode)
			if err != nil {
				if isCanceled(ctx) {
					return 499, httpHeaders, []byte("Canceled"), err
				}
				server.logger.Printf("failed to fetch tile %s %d-%d %v", name, entry.Offset, entry.Length, err)
				return 404, httpHeaders, []byte("Tile not found"), err
			}
			defer r.Close()
			b, err := io.ReadAll(r)
			if err != nil {
				status = "error"
				if isCanceled(ctx) {
					return 499, httpHeaders, []byte("Canceled"), nil
				}
				return 500, httpHeaders, []byte("I/O error"), nil
			}

			httpHeaders["ETag"] = generateEtag(b)
			if headerVal, ok := headerContentType(header); ok {
				httpHeaders["Content-Type"] = headerVal
			}
			if headerVal, ok := compressionToString(header.TileCompression); ok {
				httpHeaders["Content-Encoding"] = headerVal
			}
			return 200, httpHeaders, b, nil
		}
		dirOffset = header.LeafDirectoryOffset + entry.Offset
		dirLen = uint64(entry.Length)
	}
	return 204, httpHeaders, nil, nil
}

func isRefreshRequiredError(err error) bool {
	var refreshErr *RefreshRequiredError
	return errors.As(err, &refreshErr)
}

func isCanceled(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled)
}

var tilePattern = regexp.MustCompile(`^\/([-A-Za-z0-9_\/!-_\.\*'\(\)']+)\/(\d+)\/(\d+)\/(\d+)\.([a-z]+)$`)
var metadataPattern = regexp.MustCompile(`^\/([-A-Za-z0-9_\/!-_\.\*'\(\)']+)\/metadata$`)
var tileJSONPattern = regexp.MustCompile(`^\/([-A-Za-z0-9_\/!-_\.\*'\(\)']+)\.json$`)

func parseTilePath(path string) (bool, string, uint8, uint32, uint32, string) {
	if res := tilePattern.FindStringSubmatch(path); res != nil {
		name := res[1]
		z, _ := strconv.ParseUint(res[2], 10, 8)
		x, _ := strconv.ParseUint(res[3], 10, 32)
		y, _ := strconv.ParseUint(res[4], 10, 32)
		ext := res[5]
		return true, name, uint8(z), uint32(x), uint32(y), ext
	}
	return false, "", 0, 0, 0, ""
}

func parseTilejsonPath(path string) (bool, string) {
	if res := tileJSONPattern.FindStringSubmatch(path); res != nil {
		name := res[1]
		return true, name
	}
	return false, ""
}

func parseMetadataPath(path string) (bool, string) {
	if res := metadataPattern.FindStringSubmatch(path); res != nil {
		name := res[1]
		return true, name
	}
	return false, ""
}

func (server *Server) get(ctx context.Context, unsanitizedPath string) (archive, handler string, status int, headers map[string]string, data []byte) {
	handler = ""
	archive = ""
	headers = make(map[string]string)

	if ok, key, z, x, y, ext := parseTilePath(unsanitizedPath); ok {
		archive, handler = key, "tile"
		status, headers, data = server.getTile(ctx, headers, key, z, x, y, ext)
	} else if ok, key := parseTilejsonPath(unsanitizedPath); ok {
		archive, handler = key, "tilejson"
		status, headers, data = server.getTileJSON(ctx, headers, key)
	} else if ok, key := parseMetadataPath(unsanitizedPath); ok {
		archive, handler = key, "metadata"
		status, headers, data = server.getMetadata(ctx, headers, key)
	} else if unsanitizedPath == "/" {
		handler, status, data = "/", 204, []byte{}
	} else {
		handler, status, data = "404", 404, []byte("Path not found")
	}
	server.metrics.updateCacheStats(server.cache.Len())
	return
}

// Get a response for the given path.
// Return status code, HTTP headers, and body.
func (server *Server) Get(ctx context.Context, path string) (int, map[string]string, []byte) {
	tracker := server.metrics.startRequest()
	archive, handler, status, headers, data := server.get(ctx, path)
	tracker.finish(ctx, archive, handler, status, len(data), true)
	return status, headers, data
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

// Serve an HTTP response from the archive
func (server *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) int {
	tracker := server.metrics.startRequest()

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(405)
		tracker.finish(r.Context(), "", r.Method, 405, 0, false)
		return 405
	}

	archive, handler, statusCode, headers, body := server.get(r.Context(), r.URL.Path)
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	if statusCode == 200 {
		lrw := &loggingResponseWriter{w, 200}
		// handle if-match, if-none-match request headers based on response etag
		http.ServeContent(
			lrw, r,
			"",                // name used to infer content-type, but we've already set that
			time.UnixMilli(0), // ignore setting last-modified time and handling if-modified-since headers
			bytes.NewReader(body),
		)
		statusCode = lrw.statusCode
	} else {
		w.WriteHeader(statusCode)
		w.Write(body)
	}
	tracker.finish(r.Context(), archive, handler, statusCode, len(body), true)

	return statusCode
}

func NewCors(corsOrigins string) *cors.Cors {
	return cors.New(cors.Options{
		AllowedMethods: []string{http.MethodGet, http.MethodHead},
		AllowedOrigins: strings.Split(corsOrigins, ","),
	})
}
