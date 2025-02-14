package httpserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/filecoin-project/lassie/pkg/build"
	"github.com/filecoin-project/lassie/pkg/heyfil"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/file"

	dagpb "github.com/ipld/go-codec-dagpb"

	"github.com/filecoin-project/lassie/internal/db"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	"github.com/multiformats/go-multicodec"
)

// Add a helper function to set CORS headers
func setCorsHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Request-Id")
}

func IpfsHandler(fetcher types.Fetcher, cfg HttpServerConfig) func(http.ResponseWriter, *http.Request) {
	return func(res http.ResponseWriter, req *http.Request) {
		// Set CORS headers for every response

		fmt.Println("Recieved request with url path: ", req.URL)
		unescapedPath, err := url.PathUnescape(req.URL.Path)
		if err != nil {
			logger.Warnf("error unescaping path: %s", err)
			unescapedPath = req.URL.Path
		}
		statusLogger := newStatusLogger(req.Method, unescapedPath)

		if !checkGet(req, res, statusLogger) {
			return
		}

		ok, request := decodeRetrievalRequest(cfg, res, req, unescapedPath, statusLogger)
		if !ok {
			return
		}

		fileSize, err := decodeFileSize(req)
		if err != nil {
			errorResponse(res, statusLogger, http.StatusBadRequest, err)
			return
		}

		ok, fileName := decodeFilename(res, req, statusLogger, request.Root)
		if !ok {
			return
		}

		// TODO: this needs to be propagated through the request, perhaps on
		// RetrievalRequest or we decode it as a UUID and override RetrievalID?
		requestId := req.Header.Get("X-Request-Id")
		if requestId == "" {
			requestId = request.RetrievalID.String()
		} else {
			logger.Debugw("custom X-Request-Id fore retrieval", "request_id", requestId, "retrieval_id", request.RetrievalID)
		}
		pipeReader, pipeWriter := io.Pipe()


		// tempStore := storage.NewDeferredStorageCar(cfg.TempDir, request.Root)
		// var carWriter storage.DeferredWriter
		// if request.Duplicates {
		// 	carWriter = *storage.NewDuplicateAdderCarForStream(req.Context(), res, request.Root, request.Path, request.Scope, request.Bytes, tempStore)
		// }
		// // else {
		// // 	carWriter = deferred.NewDeferredCarWriterForStream(writer, []cid.Cid{request.Root})
		// // }
		// carStore := storage.NewCachingTempStore(carWriter.BlockWriteOpener(), tempStore)
		// defer func() {
		// 	if err := carStore.Close(); err != nil {
		// 		logger.Errorf("error closing temp store: %s", err)
		// 	}
		// }()

		// request.LinkSystem.SetWriteStorage(carStore)
		// request.LinkSystem.SetReadStorage(carStore)

		newStore := storage.NewExtractStorageCar(pipeWriter, request.Root) 
		request.LinkSystem.SetWriteStorage(newStore)
		request.LinkSystem.SetReadStorage(newStore)


		// setup preload storage for bitswap, the temporary CAR store can set up a
		// separate preload space in its storage
		// request.PreloadLinkSystem = cidlink.DefaultLinkSystem()
		// preloadStore := carStore.PreloadStore()
		// request.PreloadLinkSystem.SetReadStorage(preloadStore)
		// request.PreloadLinkSystem.SetWriteStorage(preloadStore)
		// request.PreloadLinkSystem.TrustedStorage = true

		// bytesWritten will be closed once we've started writing CAR content to
		// the response writer. Once closed, no other content should be written.
		bytesWritten := make(chan struct{}, 1)

		newStore.OnPut(func() {
			// called once we start writing blocks into the CAR (on the first Put())
			setCorsHeaders(res)
			res.Header().Set("Server", build.UserAgent) // "lassie/vx.y.z-<git commit hash>"
			res.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
			res.Header().Set("Accept-Ranges", "none")
			res.Header().Set("Cache-Control", trustlesshttp.ResponseCacheControlHeader)
			// Update: set Content-Type based on the file extension from fileName.
			ct := mime.TypeByExtension(filepath.Ext(fileName))
			if ct == "" {
				ct = "application/octet-stream"
			}
			res.Header().Set("Content-Type", ct)
			res.Header().Set("Etag", request.Etag())
			res.Header().Set("X-Content-Type-Options", "nosniff")
			res.Header().Set("X-Ipfs-Path", trustlessutils.PathEscape(unescapedPath))
			res.Header().Set("X-Trace-Id", requestId)
			if fileSize > 0 {
				res.Header().Set("Content-Length", strconv.FormatInt(fileSize, 10))
			}
			statusLogger.logStatus(200, "OK")
			close(bytesWritten)
		})

		var extractWg sync.WaitGroup
		extractWg.Add(1)
		var newExtractStore *storage.StdinReadStorage
        go func() {
			defer extractWg.Done()

			ls := cidlink.DefaultLinkSystem()
			ls.TrustedStorage = true
			unixfsnode.AddUnixFSReificationToLinkSystem(&ls)
			newExtractStore, err = storage.NewStdinReadStorage(req.Context(), pipeReader)
			if err == io.EOF {
				logger.Infof("EOF received, no-candidates")
				errorResponse(res, statusLogger, http.StatusBadGateway, errors.New("no candidates found"))
				pipeReader.Close()
				pipeWriter.Close()
				return
			}
			if err != nil {
				logger.Errorf("error creating new storage: %s", err)
				errorResponse(res, statusLogger, http.StatusInternalServerError, err)
				return
			}
			ls.SetReadStorage(newExtractStore)
			pbn, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: request.Root}, dagpb.Type.PBNode)
			if err != nil {
				logger.Error(err)
				errorResponse(res, statusLogger, http.StatusInternalServerError, err)
				return 
			}
			pbnode := pbn.(dagpb.PBNode)
		
			_, err = unixfsnode.Reify(ipld.LinkContext{}, pbnode, &ls)
			if err != nil {
				logger.Error(err)
				return 
			}
			
			err = extractFile(context.Background(), &ls, pbnode, res)
			if err != nil {
			    logger.Error("error during extract and stream: %s", err)
				errorResponse(res, statusLogger, http.StatusInternalServerError, err)
			    return
				}			
			}()
			
		logger.Debugw("fetching",
			"retrieval_id", request.RetrievalID,
			"root", request.Root.String(),
			"path", request.Path,
			"dag-scope", request.Scope,
			"entity-bytes", request.Bytes,
			"dups", request.Duplicates,
			"maxBlocks", request.MaxBlocks,
		)
		stats, err := fetcher.Fetch(req.Context(), request, types.WithEventsCallback(servertimingsSubscriber(req, bytesWritten)))
		// Close the writer after fetch is complete
		// force all blocks to flush
		if cerr := newStore.Close(); cerr != nil && !errors.Is(cerr, context.Canceled) {
			logger.Infof("error closing car writer: %s", cerr)
		}
		if err := pipeWriter.Close(); err != nil {
			logger.Errorf("error closing pipe writer: %s", err)
		}
		extractWg.Wait()

		if err != nil {
			select {
			case <-bytesWritten:
				logger.Debugw("unclean close", "cid", request.Root, "retrievalID", request.RetrievalID)
				if err := closeWithUnterminatedChunk(res); err != nil {
					logger.Infow("unable to send early termination", "err", err)
				}
				return
			default:
			}
			if errors.Is(err, retriever.ErrNoCandidates) {
				errorResponse(res, statusLogger, http.StatusBadGateway, errors.New("no candidates found"))
			} else {
				errorResponse(res, statusLogger, http.StatusGatewayTimeout, fmt.Errorf("failed to extract CID: %w", err))
			}
			return
		}

		logger.Debugw("successfully fetched",
			"retrieval_id", request.RetrievalID,
			"root", request.Root.String(),
			"path", request.Path,
			"dag-scope", request.Scope,
			"entity-bytes", request.Bytes,
			"dups", request.Duplicates,
			"maxBlocks", request.MaxBlocks,
			"duration", stats.Duration,
			"bytes", stats.Size,
		)
	}
}

func checkGet(req *http.Request, res http.ResponseWriter, statusLogger *statusLogger) bool {
	// filter out everything but GET requests
	if req.Method == http.MethodGet {
		return true
	}
	res.Header().Add("Allow", http.MethodGet)
	errorResponse(res, statusLogger, http.StatusMethodNotAllowed, errors.New("method not allowed"))
	return false
}

func decodeRequest(res http.ResponseWriter, req *http.Request, unescapedPath string, cfg HttpServerConfig, statusLogger *statusLogger) (bool, trustlessutils.Request) {
	rootCid, path, err := trustlesshttp.ParseUrlPath(unescapedPath)
	if err != nil {
		if errors.Is(err, trustlesshttp.ErrPathNotFound) {
			errorResponse(res, statusLogger, http.StatusNotFound, err)
		} else if errors.Is(err, trustlesshttp.ErrBadCid) {
			errorResponse(res, statusLogger, http.StatusBadRequest, err)
		} else {
			errorResponse(res, statusLogger, http.StatusInternalServerError, err)
		}
		return false, trustlessutils.Request{}
	}

	// if Database is true, look up the real CID by pieceCid
	if cfg.Database {
		cidStr, dbErr := db.GetCidByPieceCid(rootCid.String())
		if dbErr != nil {
			errorResponse(res, statusLogger, http.StatusInternalServerError, dbErr)
			return false, trustlessutils.Request{}
		}
		if cidStr != nil {
			newCid, parseErr := cid.Parse(*cidStr)
			if parseErr == nil {
				rootCid = newCid
			} else {
				errorResponse(res, statusLogger, http.StatusInternalServerError, parseErr)
				return false, trustlessutils.Request{}
			}
		} else {
			errorResponse(res, statusLogger, http.StatusNotFound, errors.New("CID not found"))
			return false, trustlessutils.Request{}
		}
	}

	accepts, err := trustlesshttp.CheckFormat(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, trustlessutils.Request{}
	}
	// TODO: accepts[0] should be acceptable but it may be for a
	// application/ipld.vnd.raw (IsRaw()) which we don't currently support; we
	// should add support for it in the daemon and allow accepts[0] to be chosen.
	var accept trustlesshttp.ContentType
	for _, a := range accepts {
		if a.IsCar() {
			accept = a
			break
		}
	}
	if !accept.IsCar() {
		errorResponse(res, statusLogger, http.StatusNotAcceptable, fmt.Errorf("invalid Accept header or format parameter; unsupported %q", req.Header.Get("Accept")))
	}

	dagScope, err := trustlesshttp.ParseScope(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, trustlessutils.Request{}
	}

	byteRange, err := trustlesshttp.ParseByteRange(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, trustlessutils.Request{}
	}

	return true, trustlessutils.Request{
		Root:       rootCid,
		Path:       path.String(),
		Scope:      dagScope,
		Bytes:      byteRange,
		Duplicates: accept.Duplicates,
	}
}

func decodeRetrievalRequest(cfg HttpServerConfig, res http.ResponseWriter, req *http.Request, unescapedPath string, statusLogger *statusLogger) (bool, types.RetrievalRequest) {
	ok, request := decodeRequest(res, req, unescapedPath, cfg, statusLogger)
	if !ok {
		return false, types.RetrievalRequest{}
	}

	protocols, err := parseProtocols(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, types.RetrievalRequest{}
	}

	providers, err := parseProviders(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, types.RetrievalRequest{}
	}

	// extract block limit from query param as needed
	var maxBlocks uint64
	if req.URL.Query().Has("blockLimit") {
		if parsedBlockLimit, err := strconv.ParseUint(req.URL.Query().Get("blockLimit"), 10, 64); err == nil {
			maxBlocks = parsedBlockLimit
		}
	}
	// use the lowest non-zero value for block limit
	if maxBlocks == 0 || (cfg.MaxBlocksPerRequest > 0 && maxBlocks > cfg.MaxBlocksPerRequest) {
		maxBlocks = cfg.MaxBlocksPerRequest
	}

	retrievalId, err := types.NewRetrievalID()
	if err != nil {
		errorResponse(res, statusLogger, http.StatusInternalServerError, fmt.Errorf("failed to generate retrieval ID: %w", err))
		return false, types.RetrievalRequest{}
	}

	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&linkSystem)

	return true, types.RetrievalRequest{
		Request:     request,
		RetrievalID: retrievalId,
		LinkSystem:  linkSystem,
		Protocols:   protocols,
		Providers:   providers,
		MaxBlocks:   maxBlocks,
	}
}

func decodeFilename(res http.ResponseWriter, req *http.Request, statusLogger *statusLogger, root cid.Cid) (bool, string) {
	fileName, err := parseFilename(req)
	if err != nil {
		errorResponse(res, statusLogger, http.StatusBadRequest, err)
		return false, ""
	}
	// for setting Content-Disposition header based on filename url parameter
	if fileName == "" {
		fileName = fmt.Sprintf("%s%s", root, trustlesshttp.FilenameExtCar)
	}
	return true, fileName
}

// statusLogger is a logger for logging response statuses for a given request
type statusLogger struct {
	method string
	path   string
}

func newStatusLogger(method string, path string) *statusLogger {
	return &statusLogger{method, path}
}

// logStatus logs the method, path, status code and message
func (l statusLogger) logStatus(statusCode int, message string) {
	logger.Infof("%s\t%s\t%d: %s\n", l.method, l.path, statusCode, message)
}

func parseProtocols(req *http.Request) ([]multicodec.Code, error) {
	if req.URL.Query().Has("protocols") {
		return types.ParseProtocolsString(req.URL.Query().Get("protocols"))
	}
	return nil, nil
}

func parseProviders(req *http.Request) ([]types.Provider, error) {
	if req.URL.Query().Has("providers") {
		// in case we have been given filecoin actor addresses we can look them up
		// with heyfil and translate to full multiaddrs, otherwise this is a
		// pass-through
		trans, err := heyfil.Heyfil{TranslateFaddr: true}.TranslateAll(strings.Split(req.URL.Query().Get("providers"), ","))
		if err != nil {
			return nil, err
		}
		providers, err := types.ParseProviderStrings(strings.Join(trans, ","))
		if err != nil {
			return nil, errors.New("invalid providers parameter")
		}
		return providers, nil
	}
	return nil, nil
}

// errorResponse logs and replies to the request with the status code and error
func errorResponse(res http.ResponseWriter, statusLogger *statusLogger, code int, err error) {
	statusLogger.logStatus(code, err.Error())
	http.Error(res, err.Error(), code)
}

// closeWithUnterminatedChunk attempts to take control of the the http conn and terminate the stream early
func closeWithUnterminatedChunk(res http.ResponseWriter) error {
	hijacker, ok := res.(http.Hijacker)
	if !ok {
		return errors.New("unable to access hijack interface")
	}
	conn, buf, err := hijacker.Hijack()
	if err != nil {
		return fmt.Errorf("unable to access conn through hijack interface: %w", err)
	}
	if _, err := buf.Write(trustlesshttp.ResponseChunkDelimeter); err != nil {
		return fmt.Errorf("writing response chunk delimiter: %w", err)
	}
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("flushing buff: %w", err)
	}
	// attempt to close just the write side
	if err := conn.Close(); err != nil {
		return fmt.Errorf("closing write conn: %w", err)
	}
	return nil
}


// extractFile writes the extracted file content to the provided io.Writer.
func extractFile(c context.Context, ls *ipld.LinkSystem, n ipld.Node, writer io.Writer) error {
    node, err := file.NewUnixFSFile(c, n, ls)
    if err != nil {
        return err
    }
    nlr, err := node.AsLargeBytes()
    if err != nil {
        return err
    }
    _, err = io.Copy(writer, nlr)
    return err
}

func parseFilename(req *http.Request) (string, error) {
	// check if provided filename query parameter has .car extension
	if req.URL.Query().Has("filename") {
		filename := req.URL.Query().Get("filename")
		ext := filepath.Ext(filename)
		if ext == "" {
			return "", errors.New("invalid filename parameter; missing extension")
		}
		return filename, nil
	}
	return "", nil
}

func decodeFileSize(req *http.Request) (int64, error) {
	if !req.URL.Query().Has("filesize") {
		return 0, nil
	}
	val := req.URL.Query().Get("filesize")
	size, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid fileSize parameter: %w", err)
	}
	return size, nil
}