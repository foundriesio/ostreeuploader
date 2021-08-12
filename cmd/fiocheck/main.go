package main

import (
	"flag"
	"github.com/foundriesio/ostreeuploader/pkg/ostreeuploader"
	"log"
	"os"
)

var (
	DefaultServerUrl = "https://api.foundries.io/ota/ostreehub"
)

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	repo := flag.String("repo", cwd, "A path to an ostree repo")
	ostreeHubUrl := flag.String("server", DefaultServerUrl, "An URL to OSTree Hub a repo is hosted in")
	factory := flag.String("factory", "", "A Factory to a repo belongs to")
	creds := flag.String("creds", "", "A credential archive with auth material")
	apiVer := flag.String("api-version", "v1", "A version of the OSTree Hub API to communicate with")
	flag.Parse()

	var checker ostreeuploader.Checker
	if *creds != "" {
		checker, err = ostreeuploader.NewChecker(*repo, *creds, *apiVer)
	} else {
		checker, err = ostreeuploader.NewCheckerNoAuth(*repo, *ostreeHubUrl, *factory, *apiVer)
	}
	if err != nil {
		log.Fatalf("Failed to create Fio Pusher: %s\n", err.Error())
	}

	if err := checker.Check(); err != nil {
		log.Fatalf("Failed to run Fio Checker: %s\n", err.Error())
	}

	log.Printf("Checking if the repo %s is synced with Factory: %s, %s ...\n", *repo, checker.Url(), checker.Factory())
	report, err := checker.Wait()
	if err != nil {
		log.Fatalf("Failed to push repo: %s\n", err.Error())
	}

	log.Printf("Checked: %d\n", report.Checked)
	log.Printf("Not synced %d files\n", report.NotSynced)
	if report.NotSynced == 0 {
		log.Println("Repo is synced")
		os.Exit(0)
	} else {
		log.Println("Repo is not synced !!!")
		os.Exit(1)
	}
}
