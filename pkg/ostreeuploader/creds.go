package ostreeuploader

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type (
	OAuth2 struct {
		Server string `json:"server"`
		ID     string `json:"client_id"`
		Secret string `json:"client_secret"`
	}

	OSTreeInfo struct {
		Auth   OAuth2 `json:"oauth2"`
		NoAuth bool   `json:"no_auth"`
		Server struct {
			URL string `json:"server"`
		} `json:"ostree"`
	}

	OSTreeHub struct {
		URL     string
		Factory string
		Auth    *OAuth2
	}

	OAuthToken struct {
		Token   string `json:"access_token"`
		Expires uint64 `json:"expires_in"`
	}
)

const (
	treehubFile string = "treehub.json"
)

func GetOAuthToken(auth *OAuth2) (string, error) {
	authUrl := auth.Server + "/token?grant_type=client_credentials"
	form := url.Values{"grant_type": {"client_credentials"}}
	req, err := http.NewRequest("POST", authUrl, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("Failed to make a request for an oauth2 token: %s\n", err.Error())
	}
	req.SetBasicAuth(auth.ID, auth.Secret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("Failed to make a request for an oauth2 token: %s\n", err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Failed to get oauth2 token: %s\n", resp.Status)
	}
	rd, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("Failed to get oauth2 token: %s\n", err.Error())
	}
	var tok OAuthToken
	err = json.Unmarshal(rd, &tok)
	if err != nil {
		return "", fmt.Errorf("Failed to unmarshal oauth2 token: %s\n", err.Error())
	}
	return tok.Token, nil
}

func ExtractUrlAndFactory(credZip string) (*OSTreeHub, error) {
	info, err := ParseCredArchive(credZip)
	if err != nil {
		return nil, err
	}
	// e.g. https://api.foundries.io/ota/treehub/msul-dev01/api/v3/
	url, err := url.Parse(info.Server.URL)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse the server  URL: %s\n", err.Error())
	}
	// e.g. /ota/treehub/msul-dev01/api/v3/
	pathElements := strings.Split(url.Path, "/")
	factory := pathElements[3]
	return &OSTreeHub{URL: url.Scheme + "://" + url.Host, Factory: factory, Auth: &info.Auth}, err
}

func ParseCredArchive(credZip string) (*OSTreeInfo, error) {
	f, err := os.Open(credZip)
	if err != nil {
		return nil, fmt.Errorf("Failed to open the credential archive: %s, err: %s\n", credZip, err.Error())
	}
	defer f.Close()

	s, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("Failed to get stat of the credential archive: %s, err: %s\n", credZip, err.Error())
	}

	r, err := zip.NewReader(f, s.Size())
	if err != nil {
		return nil, fmt.Errorf("Failed to create a zip reader: %s\n", err.Error())
	}

	var infoFile *zip.File = nil
	for _, zipFile := range r.File {
		if zipFile.Name == treehubFile {
			infoFile = zipFile
		}
	}
	if infoFile == nil {
		return nil, fmt.Errorf("Failed to find %s file in the archive: %s\n", treehubFile, credZip)
	}

	fi, err := infoFile.Open()
	if err != nil {
		return nil, fmt.Errorf("Failed to open %s file located in the credential archive: %s\n", treehubFile, err.Error())
	}
	defer fi.Close()

	data, err := ioutil.ReadAll(fi)
	if err != nil {
		return nil, fmt.Errorf("Failed to read data from %s file located in the credential archive: %s\n", treehubFile, err.Error())
	}
	var serverInfo OSTreeInfo
	err = json.Unmarshal(data, &serverInfo)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse OSTree info json: %s\n", err.Error())
	}
	return &serverInfo, nil
}
