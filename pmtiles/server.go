package pmtiles

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/rs/cors"
)

// Defines the cache keys. We need separate keys for the header and directory entries.
const (
	headerCacheKeyPrefix = "h:"
	dirCacheKeyPrefix    = "d:"
)

// Server is an HTTP server for tiles and metadata.
type Server struct {
	bucket    Bucket
	logger    *log.Logger
	publicURL string
	metrics   *metrics
	cache     *bigcache.BigCache
	// fetchLocks prevents the thundering herd problem when multiple requests
	// concurrently miss the cache for the same resource.
	fetchLocks sync.Map
}

// Struct to hold deserialized header for caching.
// We cache the serialized version, but deserialize for use.
type cachedHeader struct {
	Header HeaderV3
	ETag   string
}

// NewServer creates a new pmtiles HTTP server.
func NewServer(bucketURL string, prefix string, logger *log.Logger, cacheSizeMB int, publicURL string) (*Server, error) {
	ctx := context.Background()
	bucketURL, _, err := NormalizeBucketKey(bucketURL, prefix, "")
	if err != nil {
		return nil, err
	}

	bucket, err := OpenBucket(ctx, bucketURL, prefix)
	if err != nil {
		return nil, err
	}

	return NewServerWithBucket(bucket, logger, cacheSizeMB, publicURL)
}

// NewServerWithBucket creates a new HTTP server for a specific Bucket interface.
func NewServerWithBucket(bucket Bucket, logger *log.Logger, cacheSizeMB int, publicURL string) (*Server, error) {
	if cacheSizeMB <= 0 {
		cacheSizeMB = 64
	}

	// Configure BigCache
	// LifeWindow: 0 means entries don't expire based on time. Eviction is size-based.
	// CleanWindow: Set to a reasonable interval if LifeWindow is used. Not needed here.
	// HardMaxCacheSize: The primary eviction trigger.
	config := bigcache.DefaultConfig(0) // Use 0 LifeWindow for size-based eviction
	config.HardMaxCacheSize = cacheSizeMB
	config.Logger = logger // Use the provided logger

	cache, err := bigcache.New(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cache: %w", err)
	}

	l := &Server{
		bucket:    bucket,
		logger:    logger,
		publicURL: publicURL,
		metrics:   createMetrics("", logger), // change scope string if there are multiple servers running in one process
		cache:     cache,
	}

	// Initialize cache metrics after cache creation
	l.metrics.initCacheStats(cacheSizeMB * 1000 * 1000)
	// Start a goroutine to periodically update cache size metrics (optional but helpful)
	go l.monitorCacheStats()

	return l, nil
}

// monitorCacheStats periodically updates Prometheus gauges for cache size and entry count.
// monitorCacheStats periodically updates Prometheus gauges for cache size and entry count.
func (server *Server) monitorCacheStats() {
	ticker := time.NewTicker(1 * time.Minute) // Update frequency
	defer ticker.Stop()
	for range ticker.C {
		// Get the current number of entries using the Len() method
		entryCount := server.cache.Len()
		// Note: BigCache doesn't directly expose current byte size easily via Stats.
		// HardMaxCacheSize provides the limit. We can estimate usage if needed,
		// but entry count is readily available via Len().
		server.metrics.updateCacheStats(0, entryCount) // Update with entry count
	}
}

// Helper to create a standardized cache key string.
// ETag is crucial for invalidation.
func createHeaderCacheKey(name, etag string) string {
	return headerCacheKeyPrefix + name + ":" + etag
}

func createDirectoryCacheKey(name, etag string, offset, length uint64) string {
	return fmt.Sprintf("%s%s:%s:%d:%d", dirCacheKeyPrefix, name, etag, offset, length)
}

// Helper to serialize header for cache storage
func serializeCachedHeader(ch *cachedHeader) ([]byte, error) {
	return json.Marshal(ch)
}

// Helper to deserialize header from cache storage
func deserializeCachedHeader(data []byte) (*cachedHeader, error) {
	var ch cachedHeader
	err := json.Unmarshal(data, &ch)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize cached header: %w", err)
	}
	return &ch, nil
}

// lock fetches the lock for a cache key, or waits if it's already locked.
// Returns true if the lock was acquired, false if it was already locked and waited.
// The caller *must* call unlock(key) when done.
func (server *Server) lock(key string) (waitChan chan struct{}, acquired bool) {
	// sync.Map ensures atomic LoadOrStore
	waitChanUntyped, loaded := server.fetchLocks.LoadOrStore(key, make(chan struct{}))
	waitChan = waitChanUntyped.(chan struct{})

	if loaded {
		// Another goroutine is fetching, wait for it.
		<-waitChan
		return waitChan, false // Did not acquire the lock, just waited
	}
	// Lock acquired by this goroutine
	return waitChan, true
}

// unlock releases the lock for a cache key.
func (server *Server) unlock(key string, waitChan chan struct{}) {
	// Close the channel first to signal waiters
	close(waitChan)
	// Then remove the key from the map
	server.fetchLocks.Delete(key)
}

// Fetches and caches the header and root directory if not present.
// Returns the header, etag, a potential stale etag (if refresh is needed), and error.
func (server *Server) getOrFetchHeader(ctx context.Context, name string, requestEtag string) (header HeaderV3, etag string, staleEtag string, err error) {
	// Use requestEtag for the initial cache lookup.
	cacheKey := createHeaderCacheKey(name, requestEtag)
	entryBytes, cacheErr := server.cache.Get(cacheKey)

	// Cache Hit
	if cacheErr == nil {
		server.metrics.cacheRequest(name, "root", "hit")
		cachedHdr, deserErr := deserializeCachedHeader(entryBytes)
		if deserErr == nil {
			return cachedHdr.Header, cachedHdr.ETag, "", nil
		}
		// If deserialization fails, treat as miss
		server.logger.Printf("Error deserializing cached header for %s: %v", name, deserErr)
		// Explicitly delete potentially corrupted entry
		_ = server.cache.Delete(cacheKey) // Ignore error on delete
	} else if !errors.Is(cacheErr, bigcache.ErrEntryNotFound) {
		// Log unexpected cache errors but proceed as a cache miss
		server.logger.Printf("Cache get error for header %s: %v", name, cacheErr)
	}

	// Cache Miss or error
	server.metrics.cacheRequest(name, "root", "miss")

	// Lock to prevent thundering herd for the *header* fetch itself.
	// Use a distinct lock key for the header fetch process.
	headerFetchLockKey := "fetchlock:" + name + ":" + requestEtag
	waitChan, acquired := server.lock(headerFetchLockKey)
	if !acquired {
		// Waited for another fetcher, re-check cache
		entryBytes, cacheErr = server.cache.Get(cacheKey)
		if cacheErr == nil {
			server.metrics.cacheRequest(name, "root", "hit") // Hit after waiting
			cachedHdr, deserErr := deserializeCachedHeader(entryBytes)
			if deserErr == nil {
				return cachedHdr.Header, cachedHdr.ETag, "", nil
			}
			server.logger.Printf("Error deserializing cached header for %s after wait: %v", name, deserErr)
			_ = server.cache.Delete(cacheKey) // Ignore error
			// Fall through to fetch again if deserialization failed
		} else if !errors.Is(cacheErr, bigcache.ErrEntryNotFound) {
			server.logger.Printf("Cache get error for header %s after wait: %v", name, cacheErr)
			// Fall through to fetch again
		}
		// If still not found after waiting, the original fetcher likely failed.
		// Need to attempt fetch again. Re-acquire lock.
		waitChan, acquired = server.lock(headerFetchLockKey)
		if !acquired {
			// This should not happen often, indicates contention or rapid failure/retry.
			return HeaderV3{}, "", "", fmt.Errorf("failed to acquire header fetch lock after waiting")
		}
	}

	// Acquired lock - proceed with fetch
	defer server.unlock(headerFetchLockKey, waitChan)

	status := ""
	tracker := server.metrics.startBucketRequest(name, "root")
	defer func() { tracker.finish(ctx, status) }()

	server.logger.Printf("fetching header %s (Etag: %s)", name, requestEtag)
	// Fetch first 16KB to get header and potentially root directory
	r, resultEtag, statusCode, fetchErr := server.bucket.NewRangeReaderEtag(ctx, name+".pmtiles", 0, 16384, requestEtag)
	status = strconv.Itoa(statusCode)

	if fetchErr != nil {
		if isRefreshRequiredError(fetchErr) {
			// ETag mismatch. Return the stale ETag so caller can retry.
			server.logger.Printf("header fetch for %s failed: ETag mismatch (stale: %s)", name, requestEtag)
			return HeaderV3{}, "", requestEtag, nil // No error, but indicate refresh needed
		}
		server.logger.Printf("failed to fetch header %s: %v", name, fetchErr)
		return HeaderV3{}, "", "", fmt.Errorf("failed to fetch header: %w", fetchErr)
	}
	defer r.Close()

	// Read the initial chunk
	initialBytes, readErr := io.ReadAll(r)
	if readErr != nil {
		status = "error"
		server.logger.Printf("failed to read header bytes %s: %v", name, readErr)
		return HeaderV3{}, "", "", fmt.Errorf("failed to read header bytes: %w", readErr)
	}

	// Deserialize Header
	fetchedHeader, hdrErr := DeserializeHeader(initialBytes[0:HeaderV3LenBytes])
	if hdrErr != nil {
		status = "error"
		server.logger.Printf("parsing header failed for %s: %v", name, hdrErr)
		return HeaderV3{}, "", "", fmt.Errorf("failed to parse header: %w", hdrErr)
	}

	// Cache the Header
	ch := &cachedHeader{Header: fetchedHeader, ETag: resultEtag}
	serializedHeader, serErr := serializeCachedHeader(ch)
	if serErr != nil {
		server.logger.Printf("failed to serialize header for cache %s: %v", name, serErr)
		// Proceed without caching header if serialization fails
	} else {
		// Use the *resultEtag* for the new cache entry key
		newCacheKey := createHeaderCacheKey(name, resultEtag)
		setErr := server.cache.Set(newCacheKey, serializedHeader)
		if setErr != nil {
			server.logger.Printf("failed to cache header for %s: %v", name, setErr)
		}
	}

	// Cache the Root Directory (if within the initial fetch)
	if fetchedHeader.RootOffset+fetchedHeader.RootLength <= 16384 {
		rootBytes := initialBytes[fetchedHeader.RootOffset : fetchedHeader.RootOffset+fetchedHeader.RootLength]
		dirCacheKey := createDirectoryCacheKey(name, resultEtag, fetchedHeader.RootOffset, fetchedHeader.RootLength)
		setErr := server.cache.Set(dirCacheKey, rootBytes)
		if setErr != nil {
			server.logger.Printf("failed to cache root directory for %s: %v", name, setErr)
		} else {
			server.metrics.cacheRequest(name, "root_dir", "set") // Track successful cache set
		}
	} else {
		server.logger.Printf("root directory for %s not within initial 16KB fetch, will fetch separately", name)
	}

	server.logger.Printf("fetched header %s (New Etag: %s)", name, resultEtag)
	return fetchedHeader, resultEtag, "", nil
}

// Fetches and caches a directory level if not present.
// Returns the raw directory bytes, a potential stale etag, and error.
func (server *Server) getOrFetchDirectory(ctx context.Context, name, etag string, offset, length uint64, compression Compression) ([]byte, string, error) {
	cacheKey := createDirectoryCacheKey(name, etag, offset, length)
	dirBytes, cacheErr := server.cache.Get(cacheKey)

	// Cache Hit
	if cacheErr == nil {
		server.metrics.cacheRequest(name, "leaf", "hit")
		return dirBytes, "", nil
	} else if !errors.Is(cacheErr, bigcache.ErrEntryNotFound) {
		server.logger.Printf("Cache get error for directory %s: %v", name, cacheErr)
	}

	// Cache Miss
	server.metrics.cacheRequest(name, "leaf", "miss")

	// Lock to prevent thundering herd for this specific directory block
	dirFetchLockKey := "fetchlock:" + cacheKey // Use full cache key for lock
	waitChan, acquired := server.lock(dirFetchLockKey)
	if !acquired {
		// Waited, re-check cache
		dirBytes, cacheErr = server.cache.Get(cacheKey)
		if cacheErr == nil {
			server.metrics.cacheRequest(name, "leaf", "hit") // Hit after waiting
			return dirBytes, "", nil
		} else if !errors.Is(cacheErr, bigcache.ErrEntryNotFound) {
			server.logger.Printf("Cache get error for directory %s after wait: %v", name, cacheErr)
		}
		// Fall through to fetch if still not found or error
		waitChan, acquired = server.lock(dirFetchLockKey)
		if !acquired {
			return nil, "", fmt.Errorf("failed to acquire directory fetch lock after waiting")
		}
	}

	// Acquired lock - proceed with fetch
	defer server.unlock(dirFetchLockKey, waitChan)

	status := ""
	tracker := server.metrics.startBucketRequest(name, "leaf")
	defer func() { tracker.finish(ctx, status) }()

	server.logger.Printf("fetching directory %s %d-%d (Etag: %s)", name, offset, offset+length-1, etag) // Corrected log message length
	r, _, statusCode, fetchErr := server.bucket.NewRangeReaderEtag(ctx, name+".pmtiles", int64(offset), int64(length), etag)
	status = strconv.Itoa(statusCode)

	if fetchErr != nil {
		if isRefreshRequiredError(fetchErr) {
			server.logger.Printf("directory fetch for %s failed: ETag mismatch (stale: %s)", name, etag)
			return nil, etag, nil // No error, but indicate refresh needed
		}
		server.logger.Printf("failed to fetch directory %s %d-%d: %v", name, offset, length, fetchErr)
		return nil, "", fmt.Errorf("failed to fetch directory: %w", fetchErr)
	}
	defer r.Close()

	fetchedDirBytes, readErr := io.ReadAll(r)
	if readErr != nil {
		status = "error"
		server.logger.Printf("failed to read directory bytes %s %d-%d: %v", name, offset, length, readErr)
		return nil, "", fmt.Errorf("failed to read directory bytes: %w", readErr)
	}

	// Cache the fetched directory bytes
	setErr := server.cache.Set(cacheKey, fetchedDirBytes)
	if setErr != nil {
		server.logger.Printf("failed to cache directory %s %d-%d: %v", name, offset, length, setErr)
	} else {
		server.metrics.cacheRequest(name, "leaf", "set")
	}

	server.logger.Printf("fetched directory %s %d-%d", name, offset, length)
	return fetchedDirBytes, "", nil
}

// Tries to fetch header/metadata, handling potential ETag refresh.
func (server *Server) getHeaderMetadataWithRetry(ctx context.Context, name string) (header HeaderV3, metadataBytes []byte, err error) {
	header, etag, staleEtag, err := server.getOrFetchHeader(ctx, name, "")
	if err != nil {
		return // Initial fetch failed fatally
	}
	if staleEtag != "" {
		// ETag was stale, need to refetch with the *correct* (empty) ETag expectation
		server.logger.Printf("Retrying header fetch for %s due to stale ETag: %s", name, staleEtag)
		// Invalidate potentially stale cache entry before retrying
		staleHeaderKey := createHeaderCacheKey(name, staleEtag)
		_ = server.cache.Delete(staleHeaderKey) // Ignore error

		header, etag, staleEtag, err = server.getOrFetchHeader(ctx, name, "") // Retry with empty ETag
		if err != nil {
			return // Retry fetch failed fatally
		}
		if staleEtag != "" {
			// If it's *still* stale, something is very wrong (e.g., clock skew, rapid changes)
			err = fmt.Errorf("header ETag for %s changed multiple times rapidly", name)
			return
		}
	}

	// Now fetch metadata using the confirmed ETag
	status := ""
	tracker := server.metrics.startBucketRequest(name, "metadata")
	defer func() { tracker.finish(ctx, status) }()

	r, _, statusCode, fetchErr := server.bucket.NewRangeReaderEtag(ctx, name+".pmtiles", int64(header.MetadataOffset), int64(header.MetadataLength), etag)
	status = strconv.Itoa(statusCode)

	if fetchErr != nil {
		// If metadata fetch fails with ETag mismatch *now*, the header ETag we just got is already stale.
		if isRefreshRequiredError(fetchErr) {
			server.logger.Printf("Metadata fetch for %s failed: ETag mismatch after header fetch (header ETag: %s)", name, etag)
			// Invalidate the header we just cached (or tried to)
			headerCacheKey := createHeaderCacheKey(name, etag)
			_ = server.cache.Delete(headerCacheKey) // Ignore error
			// Trigger another retry cycle by returning a specific error or nil header
			err = fmt.Errorf("ETag changed between header and metadata fetch for %s", name)
			return
		}
		server.logger.Printf("failed to fetch metadata %s: %v", name, fetchErr)
		err = fmt.Errorf("failed to fetch metadata: %w", fetchErr)
		return
	}
	defer r.Close()

	decompressedBytes, mdErr := DeserializeMetadataBytes(r, header.InternalCompression)
	if mdErr != nil {
		status = "error"
		server.logger.Printf("failed to deserialize metadata %s: %v", name, mdErr)
		err = fmt.Errorf("failed to deserialize metadata: %w", mdErr)
		return
	}

	metadataBytes = decompressedBytes
	return // Success
}

func (server *Server) getTileJSON(ctx context.Context, httpHeaders map[string]string, name string) (int, map[string]string, []byte) {
	header, metadataBytes, err := server.getHeaderMetadataWithRetry(ctx, name)

	// Handle cases where header/metadata couldn't be fetched
	if err != nil {
		if strings.Contains(err.Error(), "failed to fetch header") || strings.Contains(err.Error(), "failed to parse header") {
			return 404, httpHeaders, []byte("Archive not found or invalid header")
		}
		if strings.Contains(err.Error(), "ETag changed between header and metadata") {
			// Suggest client retry
			httpHeaders["Retry-After"] = "1"
			return 503, httpHeaders, []byte("Archive refreshing, please retry")
		}
		return 500, httpHeaders, []byte("I/O Error getting metadata: " + err.Error())
	}
	// Check if header is zero value (might happen if initial fetch fails quietly)
	if header.SpecVersion == 0 {
		return 404, httpHeaders, []byte("Archive not found")
	}

	var metadataMap map[string]interface{}
	if umErr := json.Unmarshal(metadataBytes, &metadataMap); umErr != nil {
		server.logger.Printf("Error unmarshaling metadata for tilejson %s: %v", name, umErr)
		// Don't fail the request, just proceed without extra metadata fields
	}

	if server.publicURL == "" {
		return 501, httpHeaders, []byte("PUBLIC_URL must be set for TileJSON")
	}

	tilejsonBytes, tjErr := CreateTileJSON(header, metadataBytes, server.publicURL+"/"+name)
	if tjErr != nil {
		return 500, httpHeaders, []byte("Error generating tilejson: " + tjErr.Error())
	}

	httpHeaders["Content-Type"] = "application/json"
	// Use the combined header+metadata content for ETag to reflect changes in either
	combinedEtagContent := append(SerializeHeader(header), metadataBytes...)
	httpHeaders["ETag"] = generateEtag(combinedEtagContent)

	return 200, httpHeaders, tilejsonBytes
}

func (server *Server) getMetadata(ctx context.Context, httpHeaders map[string]string, name string) (int, map[string]string, []byte) {
	header, metadataBytes, err := server.getHeaderMetadataWithRetry(ctx, name)

	// Handle fetch errors similarly to getTileJSON
	if err != nil {
		if strings.Contains(err.Error(), "failed to fetch header") || strings.Contains(err.Error(), "failed to parse header") {
			return 404, httpHeaders, []byte("Archive not found or invalid header")
		}
		if strings.Contains(err.Error(), "ETag changed between header and metadata") {
			httpHeaders["Retry-After"] = "1"
			return 503, httpHeaders, []byte("Archive refreshing, please retry")
		}
		return 500, httpHeaders, []byte("I/O Error getting metadata: " + err.Error())
	}
	if header.SpecVersion == 0 {
		return 404, httpHeaders, []byte("Archive not found")
	}

	httpHeaders["Content-Type"] = "application/json"
	httpHeaders["ETag"] = generateEtag(metadataBytes) // ETag based only on metadata content

	return 200, httpHeaders, metadataBytes
}

// Main logic for fetching a tile, handles retries on ETag mismatch.
func (server *Server) getTile(ctx context.Context, httpHeaders map[string]string, name string, z uint8, x uint32, y uint32, ext string) (int, map[string]string, []byte) {
	status, headers, data, staleEtag := server.getTileAttempt(ctx, httpHeaders, name, z, x, y, ext, "")
	if staleEtag != "" {
		// ETag was stale somewhere in the fetch path. Retry the entire process.
		// The stale ETag is implicitly handled because cache lookups will miss.
		server.logger.Printf("Retrying tile fetch for %s/%d/%d/%d.%s due to stale ETag: %s", name, z, x, y, ext, staleEtag)

		// Invalidate potentially stale cache entries before retrying
		staleHeaderKey := createHeaderCacheKey(name, staleEtag)
		_ = server.cache.Delete(staleHeaderKey) // Ignore error
		// We don't know which specific directory level was stale, so we can't easily delete it.
		// The retry relies on getOrFetchHeader/Directory checking the cache again.

		status, headers, data, staleEtag = server.getTileAttempt(ctx, httpHeaders, name, z, x, y, ext, "") // Retry
		if staleEtag != "" {
			// If it's *still* stale after a retry, return an error/retry suggestion.
			server.logger.Printf("Tile fetch for %s/%d/%d/%d.%s failed again due to ETag changes", name, z, x, y, ext)
			headers["Retry-After"] = "1"
			return 503, headers, []byte("Archive refreshing, please retry")
		}
	}
	return status, headers, data
}

// Attempts to fetch a tile, returning a stale ETag if a refresh is needed.
func (server *Server) getTileAttempt(ctx context.Context, httpHeaders map[string]string, name string, z uint8, x uint32, y uint32, ext string, requestEtag string) (int, map[string]string, []byte, string) {
	// 1. Get the header (handles caching and ETag fetching internally)
	header, etag, staleEtag, err := server.getOrFetchHeader(ctx, name, requestEtag)
	if err != nil {
		// Header fetch failed fatally
		return 500, httpHeaders, []byte("I/O Error getting header: " + err.Error()), ""
	}
	if staleEtag != "" {
		// Header fetch indicated ETag mismatch, signal caller to retry
		return 500, httpHeaders, []byte("Stale ETag detected"), staleEtag
	}
	// Check if header is zero value (might happen if initial fetch fails quietly)
	if header.SpecVersion == 0 {
		return 404, httpHeaders, []byte("Archive not found"), ""
	}

	// 2. Validate request against header
	if z < header.MinZoom || z > header.MaxZoom {
		return 404, httpHeaders, []byte("Tile not found (zoom out of range)"), ""
	}

	// Validate file extension
	expectedExt := headerExt(header) // Gets ".mvt", ".png", etc. or ""
	if expectedExt != "" && "."+ext != expectedExt {
		return 400, httpHeaders, []byte(fmt.Sprintf("path mismatch: archive is type %s (%s)", tileTypeToString(header.TileType), expectedExt)), ""
	}

	// 3. Find the tile entry by traversing directories
	tileID := ZxyToID(z, x, y)
	dirOffset, dirLen := header.RootOffset, header.RootLength
	var entry EntryV3
	found := false

	for depth := 0; depth <= 3; depth++ {
		// Get the directory (handles caching and ETag fetching internally)
		dirBytes, dirStaleEtag, dirErr := server.getOrFetchDirectory(ctx, name, etag, dirOffset, dirLen, header.InternalCompression)
		if dirErr != nil {
			return 500, httpHeaders, []byte("I/O Error getting directory: " + dirErr.Error()), ""
		}
		if dirStaleEtag != "" {
			// Directory fetch indicated ETag mismatch
			return 500, httpHeaders, []byte("Stale ETag detected for directory"), dirStaleEtag
		}

		// Deserialize directory
		directory := DeserializeEntries(bytes.NewBuffer(dirBytes), header.InternalCompression)
		var ok bool
		entry, ok = FindTile(directory, tileID)

		if !ok {
			break // Tile not found in this directory branch
		}

		if entry.RunLength > 0 {
			found = true
			break // Found the actual tile entry
		}

		// It's a leaf directory entry, update offset/length and continue
		dirOffset = header.LeafDirectoryOffset + entry.Offset
		dirLen = uint64(entry.Length)
	}

	// 4. Handle tile found or not found
	if !found {
		// Note: Metric incrementing is handled by tracker.finish
		return 204, httpHeaders, nil, "" // Tile not in archive (or branch ended)
	}

	// 5. Fetch the tile data
	status := ""
	tracker := server.metrics.startBucketRequest(name, "tile")
	defer func() { tracker.finish(ctx, status) }()

	r, _, statusCode, fetchErr := server.bucket.NewRangeReaderEtag(ctx, name+".pmtiles", int64(header.TileDataOffset+entry.Offset), int64(entry.Length), etag)
	status = strconv.Itoa(statusCode)

	if fetchErr != nil {
		if isRefreshRequiredError(fetchErr) {
			// ETag mismatch on the final tile fetch
			return 500, httpHeaders, []byte("Stale ETag detected for tile data"), etag
		}
		// Possible we have header/dir cached but archive changed/disappeared
		if isCanceled(ctx) {
			return 499, httpHeaders, []byte("Canceled"), ""
		}
		server.logger.Printf("failed to fetch tile %s %d-%d: %v", name, entry.Offset, entry.Length, fetchErr)
		// Note: Metric incrementing is handled by tracker.finish
		return 500, httpHeaders, []byte(fmt.Sprintf("Tile fetch error: %v", fetchErr)), "" // More specific error
	}
	defer r.Close()

	tileBytes, readErr := io.ReadAll(r)
	if readErr != nil {
		status = "error"
		if isCanceled(ctx) {
			return 499, httpHeaders, []byte("Canceled"), ""
		}
		server.logger.Printf("failed to read tile bytes %s %d-%d: %v", name, entry.Offset, entry.Length, readErr)
		// Note: Metric incrementing is handled by tracker.finish
		return 500, httpHeaders, []byte("I/O error reading tile"), ""
	}

	// 6. Success - set headers and return data
	// Note: Metric incrementing is handled by tracker.finish
	httpHeaders["ETag"] = generateEtag(tileBytes) // ETag based on actual tile content
	if headerVal, ok := headerContentType(header); ok {
		httpHeaders["Content-Type"] = headerVal
	}
	if headerVal, ok := compressionToString(header.TileCompression); ok {
		httpHeaders["Content-Encoding"] = headerVal
	}

	return 200, httpHeaders, tileBytes, ""
}

func isRefreshRequiredError(err error) bool {
	_, ok := err.(*RefreshRequiredError)
	return ok
}

func isCanceled(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled)
}

// Regex patterns remain the same
var tilePattern = regexp.MustCompile(`^\/([-A-Za-z0-9_\/!-_\.\*'\(\)']+)\/(\d+)\/(\d+)\/(\d+)\.([a-z]+)$`)
var metadataPattern = regexp.MustCompile(`^\/([-A-Za-z0-9_\/!-_\.\*'\(\)']+)\/metadata$`)
var tileJSONPattern = regexp.MustCompile(`^\/([-A-Za-z0-9_\/!-_\.\*'\(\)']+)\.json$`)

// Parse functions remain the same
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

// Top-level routing logic
func (server *Server) routeRequest(ctx context.Context, unsanitizedPath string) (archive, handler string, status int, headers map[string]string, data []byte) {
	handler = ""
	archive = ""
	headers = make(map[string]string)
	headers["Vary"] = "Accept-Encoding" // Good practice

	if ok, key, z, x, y, ext := parseTilePath(unsanitizedPath); ok {
		archive, handler = key, "tile"
		status, headers, data = server.getTile(ctx, headers, key, z, x, y, ext)
	} else if ok, key := parseTilejsonPath(unsanitizedPath); ok {
		archive, handler = key, "tilejson"
		status, headers, data = server.getTileJSON(ctx, headers, key)
	} else if ok, key := parseMetadataPath(unsanitizedPath); ok {
		archive, handler = key, "metadata"
		status, headers, data = server.getMetadata(ctx, headers, key)
	} else if unsanitizedPath == "/" || unsanitizedPath == "" {
		// Handle root path, e.g., return health check or basic info
		handler, status, data = "/", 200, []byte("PMTiles server OK")
		headers["Content-Type"] = "text/plain"
	} else {
		handler, status, data = "404", 404, []byte("Path not found")
		headers["Content-Type"] = "text/plain"
	}

	return
}

// Get a response for the given path. Public API.
func (server *Server) Get(ctx context.Context, path string) (int, map[string]string, []byte) {
	tracker := server.metrics.startRequest()
	// Sanitize path minimally, more robust sanitization might be needed depending on deployment
	cleanPath := strings.TrimSuffix(path, "/")
	if cleanPath == "" {
		cleanPath = "/"
	}

	archive, handler, status, headers, data := server.routeRequest(ctx, cleanPath)
	tracker.finish(ctx, archive, handler, status, len(data), true)
	return status, headers, data
}

// loggingResponseWriter remains the same
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

	// Deny non-GET/HEAD requests early
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		w.WriteHeader(http.StatusMethodNotAllowed)
		tracker.finish(r.Context(), "", r.Method, http.StatusMethodNotAllowed, 0, false)
		return http.StatusMethodNotAllowed
	}

	// Route the request using the internal router
	archive, handler, statusCode, headers, body := server.routeRequest(r.Context(), r.URL.Path)

	// Set common headers
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	// Add cache control header (example: public, 1 hour) - adjust as needed
	w.Header().Set("Cache-Control", "public, max-age=3600")

	// Use http.ServeContent for proper handling of conditional requests (If-None-Match, If-Modified-Since)
	// and range requests (though range requests aren't typical for tiles/metadata).
	if statusCode == http.StatusOK {
		lrw := &loggingResponseWriter{w, http.StatusOK} // Wrap to capture final status code

		// http.ServeContent needs a ReadSeeker. bytes.NewReader provides this.
		// It also needs a modtime. Use a fixed zero time if not meaningful.
		http.ServeContent(lrw, r, "", time.Time{}, bytes.NewReader(body))

		// Update status code based on ServeContent's decision (e.g., 304 Not Modified)
		statusCode = lrw.statusCode
	} else {
		// For non-200 status codes (e.g., 404, 500, 204), write header and body directly.
		w.WriteHeader(statusCode)
		if len(body) > 0 {
			_, _ = w.Write(body) // Ignore potential write error after header sent
		}
	}

	// Record metrics with the final status code
	tracker.finish(r.Context(), archive, handler, statusCode, len(body), true)
	return statusCode
}

// NewCors remains the same
func NewCors(corsOrigins string) *cors.Cors {
	// Handle "*" specifically for AllowAll origins
	if corsOrigins == "*" {
		return cors.AllowAll()
	}

	return cors.New(cors.Options{
		AllowedMethods: []string{http.MethodGet, http.MethodHead},
		// Split origins string, trim whitespace
		AllowedOrigins: func() []string {
			origins := strings.Split(corsOrigins, ",")
			for i := range origins {
				origins[i] = strings.TrimSpace(origins[i])
			}
			return origins
		}(),
		AllowedHeaders: []string{"*"}, // Allow common headers
		MaxAge:         3600,          // Cache preflight requests for 1 hour
		// AllowCredentials: true, // Uncomment if cookies/auth needed (use with specific origins, not '*')
	})
}
