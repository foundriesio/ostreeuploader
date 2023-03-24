package ostreeuploader

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
)

type (
	Puller interface {
		OSTreeHubAccessor
		Pull(commitHash string, corId string) error
	}

	puller struct {
		ostreehub *ostreeHubAccessor
	}
)

func NewPullerWithToken(repo, hubURL, factory, token, apiVer string) (Puller, error) {
	cmd := exec.Command("ostree", "init", "--repo", repo, "--mode", "archive")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("err: %s", out)
	}
	th, err := newOSTreeHubAccessorWithToken(repo, hubURL, factory, token, apiVer)
	if err != nil {
		return nil, err
	}
	return &puller{ostreehub: th}, nil
}

func NewPuller(repo string, credFile string, apiVer string) (Puller, error) {
	cmd := exec.Command("ostree", "init", "--repo", repo, "--mode", "archive")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("err: %s", out)
	}
	th, err := newOSTreeHubAccessor(repo, credFile, apiVer)
	if err != nil {
		return nil, err
	}
	return &puller{ostreehub: th}, nil
}

func NewPullerNoAuth(repo string, hubURL string, factory string, apiVer string) (Puller, error) {
	cmd := exec.Command("ostree", "init", "--repo", repo, "--mode", "archive")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("err: %s", out)
	}
	th, err := newOSTreeHubAccessorNoAuth(repo, hubURL, factory, apiVer)
	if err != nil {
		return nil, err
	}
	return &puller{ostreehub: th}, nil
}

func (p *puller) Url() string {
	return p.ostreehub.url.String()
}

func (p *puller) Factory() string {
	return p.ostreehub.Factory()
}

func (p *puller) Pull(commitHash string, corId string) error {
	if err := p.ostreehub.auth(); err != nil {
		return err
	}
	url := *p.ostreehub.url
	url.Path = path.Join(url.Path, "download-urls")
	req, err := http.NewRequest("POST", url.String(), nil)
	if err != nil {
		log.Fatalf("Failed to create a request to get download URLs: %s\n", err.Error())
	}
	req.Header.Set("X-Correlation-ID", corId)
	if uuid, err := GetUUID(); err == nil {
		req.Header.Set("X-Request-ID", uuid)
	}
	req.Header.Set("Content-Type", "application/json")
	p.ostreehub.token.SetAuthHeader(req)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to make request to get download URLs: %s\n", err.Error())
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close a response body: %s\n", err.Error())
		}
	}()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get download URLs: %s; %s", resp.Status, string(b))
	}

	type Response struct {
		DownloadUrl string `json:"download_url"`
		AccessToken string `json:"access_token"`
	}
	var res []Response
	err = json.Unmarshal(b, &res)
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("failed to get download URLs: download URL list is empty")
	}
	// we just get the first download URL, might wanna add some logic to determine the closest GCS server/bucket
	downloadOriginIndx := len(res) - 1
	log.Printf("download URL: %s\n", res[downloadOriginIndx].DownloadUrl)
	cmd := exec.Command("ostree", "remote", "add", "--force", "--repo", p.ostreehub.repo, "--no-gpg-verify", "gcs", res[downloadOriginIndx].DownloadUrl)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("err: %s", out)
	}

	cmd = exec.Command("ostree", "pull", "--repo", p.ostreehub.repo,
		fmt.Sprintf("--http-header=Authorization=Bearer %s", res[downloadOriginIndx].AccessToken), "gcs", commitHash)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return err
	}
	return nil
}
