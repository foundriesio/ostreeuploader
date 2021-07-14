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
	ostreeHubUrl := flag.String("server", DefaultServerUrl, "An URL to OSTree Hub to upload repo to")
	factory := flag.String("factory", "", "A Factory to upload repo for")
	creds := flag.String("creds", "", "A credential archive with auth material")
	summary := flag.Bool("summary", false, "A flag to turn on a summary update at the end of repo push")
	flag.Parse()

	var pusher ostreeuploader.Pusher
	if *creds != "" {
		pusher, err = ostreeuploader.NewPusher(*repo, *creds)
	} else {
		pusher, err = ostreeuploader.NewPusherNoAuth(*repo, *ostreeHubUrl, *factory)
	}
	if err != nil {
		log.Fatalf("Failed to create Fio Pusher: %s\n", err.Error())
	}

	if err := pusher.Push(); err != nil {
		log.Fatalf("Failed to run Fio Pusher: %s\n", err.Error())
	}

	log.Printf("Pushing %s to %s, factory: %s ...\n", *repo, pusher.Url(), pusher.Factory())
	report, err := pusher.Wait()
	if err != nil {
		log.Fatalf("Failed to push repo: %s\n", err.Error())
	}

	log.Printf("Checked: %d\n", report.Checked)
	log.Printf("Sent %d files, %d objects, %d bytes\n", report.Sent.FileNumb, report.Sent.ObjNumb, report.Sent.Bytes)
	log.Printf("Uploaded %d files, synced %d objects, uploaded to GCS %d objects\n",
		report.Synced.UploadedFileNumb, report.Synced.SyncedFileNumb, report.Synced.UploadSyncedFileNumb)
	log.Printf("Failed to sync %d objects", report.Synced.SyncFailedNumb)

	if *summary {
		log.Println("Updating summary...")
		err := pusher.UpdateSummary()
		if err != nil {
			log.Printf("Failed to update summary: %s\n", err.Error())
		} else {
			log.Printf("Summary has been successfully updated")
		}
	}
}
