package ostreeuploader

import (
	"fmt"
)

type (
	RepoFile struct {
		Path  string
		CRC32 uint32
	}

	CheckStatus struct {
		Check     <-chan uint
		NotSynced <-chan *RepoFile
	}

	CheckReport struct {
		Checked   uint
		NotSynced uint
	}
)

type (
	checker struct {
		ostreehub *ostreeHubAccessor
		status    *CheckStatus
		filter    []string
	}
)

var (
	checkFileFilterInV1 = []string{
		"./objects/",
	}
	checkFileFilterInV2 = []string{
		"./objects/",
		"./config",
	}
)

func NewChecker(repo string, credFile string, apiVer string) (Checker, error) {
	th, err := newOSTreeHubAccessor(repo, credFile, apiVer)
	if err != nil {
		return nil, err
	}
	return &checker{ostreehub: th, filter: func() []string {
		if apiVer == "v2" {
			return checkFileFilterInV2
		} else {
			return checkFileFilterInV1
		}
	}()}, nil
}

func NewCheckerNoAuth(repo string, hubURL string, factory string, apiVer string) (Checker, error) {
	th, err := newOSTreeHubAccessorNoAuth(repo, hubURL, factory, apiVer)
	if err != nil {
		return nil, err
	}
	return &checker{ostreehub: th, filter: func() []string {
		if apiVer == "v2" {
			return checkFileFilterInV2
		} else {
			return checkFileFilterInV1
		}
	}()}, nil
}

func (p *checker) Url() string {
	return p.ostreehub.Url()
}

func (p *checker) Factory() string {
	return p.ostreehub.Factory()
}

func (p *checker) Check(corId string) error {
	if err := p.ostreehub.auth(); err != nil {
		return err
	}

	p.status = check(p.ostreehub.repo, walkAndCrcRepo(p.ostreehub.repo, p.filter), p.ostreehub.url, p.ostreehub.token, corId)
	return nil
}

func (p *checker) Wait() (*CheckReport, error) {
	if p.status == nil {
		return nil, fmt.Errorf("cannot wait for Pusher jobs completion if there are none of running jobs")
	}
	return waitForCheck(p.status), nil
}
