package ostreeuploader

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type (
	SendReport struct {
		FileNumb uint
		ObjNumb  uint
		Bytes    int64
	}

	SyncReport struct {
		UploadedFileNumb     uint32 `json:"uploaded"`
		SyncedFileNumb       uint32 `json:"synced"`
		UploadSyncedFileNumb uint32 `json:"upload_synced"`
		SyncFailedNumb       uint32 `json:"sync_failed"`
	}

	OSTreeHubAccessor interface {
		Url() string
		Factory() string
	}

	Pusher interface {
		OSTreeHubAccessor
		Push(corId string) error
		Wait() (*Report, error)
		UpdateSummary() error
	}

	Checker interface {
		OSTreeHubAccessor
		Check(corId string) error
		Wait() (*CheckReport, error)
	}

	Status struct {
		Check <-chan uint
		Send  <-chan *SendReport
		Sync  <-chan *SyncReport
	}

	Report struct {
		Checked uint
		Sent    SendReport
		Synced  SyncReport
	}
)

type (
	ostreeHubAccessor struct {
		repo  string
		url   *url.URL
		hub   *OSTreeHub
		token string
	}
	pusher struct {
		ostreehub *ostreeHubAccessor
		status    *Status
	}
)

const (
	// a single goroutine traverses an ostree repo,
	// generates CRC for each file and enqueue a file info to the queue/channel
	walkQueueSize uint = 10000
	// a number of goroutine to read from the file queue and push them to OSTreeHub
	// each goroutine at first checks if given files are already present on GCS and uploads
	// only those files/objects that are missing or CRC is not equal
	concurrentPusherNumb int = 20
	// maximum number of files to check per a single HTTP request
	filesToCheckMaxNumb int = 500
	// maximum file size
	maxFileSize int64 = 1024 * 1024 * 200 //200 MB
)

var (
	repoFileFilterIn = []string{
		"./objects/",
		"./config",
		"./refs/",
		"./deltas/",
		"./delta-indexes/",
	}
)

func newOSTreeHubAccessor(repo string, credFile string, apiVer string) (*ostreeHubAccessor, error) {
	if err := checkRepoDir(repo); err != nil {
		return nil, err
	}
	hub, err := ExtractUrlAndFactory(credFile)
	if err != nil {
		return nil, err
	}
	reqUrl, err := url.Parse(hub.URL + "/ota/ostreehub/" + hub.Factory + "/" + apiVer + "/repos/lmp")
	if err != nil {
		return nil, err
	}

	return &ostreeHubAccessor{repo: repo, url: reqUrl, hub: hub, token: ""}, nil
}

func newOSTreeHubAccessorNoAuth(repo string, hubURL string, factory string, apiVer string) (*ostreeHubAccessor, error) {
	if err := checkRepoDir(repo); err != nil {
		return nil, err
	}
	if hubURL == "" {
		return nil, fmt.Errorf("URL to OSTreehub is not specified")
	}
	if factory == "" {
		return nil, fmt.Errorf("factory name is not specified")
	}
	hub := OSTreeHub{
		URL:     hubURL,
		Factory: factory,
	}
	reqUrl, err := url.Parse(hub.URL + "/" + apiVer + "/repos/lmp?factory=" + hub.Factory)
	if err != nil {
		return nil, err
	}
	return &ostreeHubAccessor{repo: repo, url: reqUrl, hub: &hub, token: ""}, nil
}

func NewPusher(repo string, credFile string, apiVer string) (Pusher, error) {
	th, err := newOSTreeHubAccessor(repo, credFile, apiVer)
	if err != nil {
		return nil, err
	}
	return &pusher{ostreehub: th}, nil
}

func NewPusherNoAuth(repo string, hubURL string, factory string, apiVer string) (Pusher, error) {
	th, err := newOSTreeHubAccessorNoAuth(repo, hubURL, factory, apiVer)
	if err != nil {
		return nil, err
	}
	return &pusher{ostreehub: th}, nil
}

func (p *ostreeHubAccessor) Url() string {
	return p.hub.URL
}

func (p *ostreeHubAccessor) Factory() string {
	return p.hub.Factory
}

func (p *pusher) Url() string {
	return p.ostreehub.Url()
}

func (p *pusher) Factory() string {
	return p.ostreehub.Factory()
}

func (p *pusher) Push(corId string) error {
	if err := p.ostreehub.auth(); err != nil {
		return err
	}

	if p.status != nil {
		return fmt.Errorf("cannot run Pusher if there are unfinished push jobs")
	}
	p.status = push(p.ostreehub.repo, walkAndCrcRepo(p.ostreehub.repo, repoFileFilterIn), p.ostreehub.url, p.ostreehub.token, corId)
	return nil
}

func (p *pusher) Wait() (*Report, error) {
	if p.status == nil {
		return nil, fmt.Errorf("cannot wait for Pusher jobs completion if there are none of running jobs")
	}
	return wait(p.status), nil
}

func (p *pusher) UpdateSummary() error {
	u := *p.ostreehub.url
	u.Path += "/summary"
	return updateSummary(&u, p.ostreehub.token)
}

func checkRepoDir(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return fmt.Errorf("The specified directory doesn't exist: %s\n", dir)
	}
	if _, err := os.Stat(path.Join(dir, "config")); os.IsNotExist(err) {
		return fmt.Errorf("The specified directory doesn't contain an ostree repo: %s\n", dir)
	}

	_, objErr := os.Stat(path.Join(dir, "objects"))
	_, deltaErr := os.Stat(path.Join(dir, "deltas"))
	if os.IsNotExist(objErr) && os.IsNotExist(deltaErr) {
		return fmt.Errorf("The specified directory doesn't contain neither ostree repo objects nor deltas: %s\n", dir)
	}
	return nil
}

func (p *ostreeHubAccessor) auth() error {
	if p.hub.Auth == nil {
		return nil
	}
	t, err := GetOAuthToken(p.hub.Auth)
	if err != nil {
		return err
	}
	log.Printf("OAuth token has been successfully obtained at %s\n", p.hub.Auth.Server)
	p.token = t
	return nil
}

func walkAndCrcRepo(repoDir string, filter []string) <-chan *RepoFile {
	dir := filepath.Clean(repoDir)
	queue := make(chan *RepoFile, walkQueueSize)
	go func() {
		defer close(queue)
		table := crc32.MakeTable(crc32.Castagnoli)
		hasher := crc32.New(table)

		if err := filepath.Walk(dir, func(fullPath string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				log.Fatalf("Failed to walk through a repo: %s\n", walkErr.Error())
			}
			if !info.IsDir() && maxFileSize < info.Size() {
				log.Fatalf("Found a file in the repo that exceeds the maximum allowed file size: %s; %d > %d\n",
					fullPath, info.Size(), maxFileSize)
			}
			if info.IsDir() {
				return nil
			}
			relPath := strings.Replace(fullPath, dir, ".", 1)
			if !filterRepoFiles(relPath, filter) {
				return nil
			}

			f, err := os.Open(fullPath)
			if err != nil {
				log.Fatalf("Failed to open file: %s\n", err.Error())
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			hasher.Reset()
			w, err := io.Copy(hasher, f)
			if err != nil {
				log.Fatalf("Failed to write file data to CRC hasher: %s\n", err.Error())
			}
			if w != info.Size() {
				log.Fatalf("Invalid amount of data written to CRC hasher: %s\n", err.Error())
			}
			crc := hasher.Sum32()
			queue <- &RepoFile{Path: relPath, CRC32: crc}
			return nil
		}); err != nil {
			log.Fatalf("Failed to walk through a repo directory: %s\n", err.Error())
		}
	}()
	return queue
}

func filterRepoFiles(path string, filter []string) bool {
	for _, f := range filter {
		if strings.HasPrefix(path, f) {
			return true
		}
	}
	return false
}

func push(repoDir string, fileQueue <-chan *RepoFile, url *url.URL, token string, corId string) *Status {
	checkReportQueue := make(chan uint, concurrentPusherNumb)
	reportQueue := make(chan *SendReport, concurrentPusherNumb)
	recvReportQueue := make(chan *SyncReport, concurrentPusherNumb)

	go func() {
		var wg sync.WaitGroup
		for ii := 0; ii < concurrentPusherNumb; ii++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					objectsToCheck := make(map[string]uint32)

					for object := range fileQueue {
						objectsToCheck[object.Path] = object.CRC32
						if len(objectsToCheck) > filesToCheckMaxNumb {
							break
						}
					}

					if len(objectsToCheck) == 0 {
						break
					}

					objectsToSync := checkRepo(objectsToCheck, url, token, corId)

					checkReportQueue <- uint(len(objectsToCheck))

					if len(objectsToSync) > 0 {
						tarReader, sendReportChannel := Tar(repoDir, objectsToSync)
						recvReportChannel := pushRepo(tarReader, url, token, corId)

						reportQueue <- <-sendReportChannel
						recvReportQueue <- <-recvReportChannel
					}
				}
			}()
		}
		wg.Wait()
		close(checkReportQueue)
		close(reportQueue)
		close(recvReportQueue)
	}()
	return &Status{Check: checkReportQueue, Send: reportQueue, Sync: recvReportQueue}
}

func check(repoDir string, fileQueue <-chan *RepoFile, url *url.URL, token string, corId string) *CheckStatus {
	checkReportQueue := make(chan uint, concurrentPusherNumb)
	statusQueue := make(chan *RepoFile, concurrentPusherNumb)

	go func() {
		var wg sync.WaitGroup
		for ii := 0; ii < concurrentPusherNumb; ii++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					objectsToCheck := make(map[string]uint32)

					for object := range fileQueue {
						objectsToCheck[object.Path] = object.CRC32
						if len(objectsToCheck) > filesToCheckMaxNumb {
							break
						}
					}

					if len(objectsToCheck) == 0 {
						break
					}

					objectsToSync := checkRepo(objectsToCheck, url, token, corId)
					checkReportQueue <- uint(len(objectsToCheck))

					for file, crc := range objectsToSync {
						statusQueue <- &RepoFile{Path: file, CRC32: crc}
					}
				}
			}()
		}
		wg.Wait()
		close(checkReportQueue)
		close(statusQueue)
	}()
	return &CheckStatus{Check: checkReportQueue, NotSynced: statusQueue}
}

func waitForCheck(status *CheckStatus) *CheckReport {
	var totalChecked uint
	var totalNotSynced uint
	for {
		select {
		case checked, ok := <-status.Check:
			if !ok {
				continue
			}
			totalChecked += checked

		case notSynced, ok := <-status.NotSynced:
			if !ok {
				log.Println("Repo check has completed")
				return &CheckReport{Checked: totalChecked, NotSynced: totalNotSynced}
			}
			totalNotSynced += 1
			log.Printf("%s\n", notSynced.Path)
		}
	}
}

func checkRepo(objs map[string]uint32, url *url.URL, token string, corId string) map[string]uint32 {
	jsonObjects, _ := json.Marshal(objs)
	req, err := http.NewRequest("GET", url.String(), bytes.NewBuffer(jsonObjects))
	if err != nil {
		log.Fatalf("Failed to create a request to check objects presence: %s\n", err.Error())
	}
	req.Header.Set("X-Correlation-ID", corId)
	if uuid, err := GetUUID(); err == nil {
		req.Header.Set("X-Request-ID", uuid)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to make request to check objects presence: %s\n", err.Error())
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close a response body: %s\n", err.Error())
		}
	}()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read response: %s\n", err.Error())
	}

	respMap := map[string]uint32{}
	if err := json.Unmarshal(body, &respMap); err != nil {
		log.Fatalf("Failed to read response: %s\n", err.Error())
	}
	return respMap
}

func pushRepo(pr *io.PipeReader, u *url.URL, token string, corId string) <-chan *SyncReport {
	req := &http.Request{
		Method:           "PUT",
		ProtoMajor:       1,
		ProtoMinor:       1,
		URL:              u,
		TransferEncoding: []string{"chunked"},
		Body:             pr,
		Header:           make(map[string][]string),
	}

	req.Header.Set("X-Correlation-ID", corId)
	if uuid, err := GetUUID(); err == nil {
		req.Header.Set("X-Request-ID", uuid)
	}
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	//TODO: timeout
	client := &http.Client{}
	client.Transport = &http.Transport{DisableCompression: false,
		WriteBufferSize: 1024 * 1025 * 10, ReadBufferSize: 1024 * 1024 * 10}

	reportChannel := make(chan *SyncReport, 1)
	go func() {
		defer close(reportChannel)
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		} else {
			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Filed to read response: %s\n", err.Error())
			}
			var status SyncReport
			if err := json.Unmarshal(body, &status); err != nil {
				log.Printf("Filed to umarshal response: %s\n", err.Error())
			}
			reportChannel <- &status
		}
	}()
	return reportChannel
}

func wait(statusQueue *Status) *Report {
	var totalChecked uint
	var totalSendReport SendReport
	var totalRecvReport SyncReport
	for {
		select {
		case checked, ok := <-statusQueue.Check:
			if !ok {
				continue
			}
			totalChecked += checked
			log.Printf("Checked: %d\n", totalChecked)

		case sendReport, ok := <-statusQueue.Send:
			if !ok || sendReport == nil {
				continue
			}
			totalSendReport.FileNumb += sendReport.FileNumb
			totalSendReport.ObjNumb += sendReport.ObjNumb
			totalSendReport.Bytes += sendReport.Bytes
			log.Printf("Sent: %d\n", totalSendReport.FileNumb)

		case recvReport, ok := <-statusQueue.Sync:
			if !ok {
				log.Println("Repo sync has completed")
				return &Report{totalChecked, totalSendReport, totalRecvReport}
			}
			totalRecvReport.UploadedFileNumb += recvReport.UploadedFileNumb
			totalRecvReport.SyncedFileNumb += recvReport.SyncedFileNumb
			totalRecvReport.UploadSyncedFileNumb += recvReport.UploadSyncedFileNumb
			totalRecvReport.SyncFailedNumb += recvReport.SyncFailedNumb
		}
	}
}

func updateSummary(url *url.URL, token string) error {
	req, err := http.NewRequest("PUT", url.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	d, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Failed to update summary: %s, failed to read response body\n", resp.Status)
	}
	return fmt.Errorf("Failed to update summary: %s, %s\n", resp.Status, string(d))
}

func GetUUID() (string, error) {
	uuid := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, uuid[:])
	if err != nil {
		return "", err
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // Version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // Variant is 10

	buf := make([]byte, 36)
	hex.Encode(buf, uuid[:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], uuid[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], uuid[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], uuid[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], uuid[10:])

	return string(buf), nil
}
