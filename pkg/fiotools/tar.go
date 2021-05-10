package fiotools

import (
	"archive/tar"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

func Tar(repoDir string, files map[string]uint32) (*io.PipeReader, <-chan *SendReport) {
	pr, pw := io.Pipe()
	reportChannel := make(chan *SendReport, 1)
	go func() {
		defer pw.Close()
		tw := tar.NewWriter(pw)
		defer tw.Close()
		defer close(reportChannel)
		var sr SendReport
		for file, crc := range files {
			f, err := os.Open(path.Join(repoDir, file))
			if err != nil {
				panic(err)
			}
			fileInfo, err := f.Stat()
			if err != nil {
				panic(err)
			}
			hdr, err := tar.FileInfoHeader(fileInfo, "")
			if err != nil {
				panic(err)
			}
			hdr.Name = file
			hdr.Format = tar.FormatPAX
			hdr.PAXRecords = map[string]string{"FIO.ostree.CRC": strconv.FormatUint(uint64(crc), 10)}
			if err := tw.WriteHeader(hdr); err != nil {
				panic(err)
			}
			if fileInfo.IsDir() {
				f.Close()
				continue
			}
			w, err := io.Copy(tw, f)
			if err != nil {
				f.Close()
				panic(err)
			}
			tw.Flush()
			f.Close()

			if strings.HasPrefix(file, "./objects") {
				sr.ObjNumb += 1
			}
			sr.FileNumb += 1
			sr.Bytes += w
		}
		reportChannel <- &sr
	}()
	return pr, reportChannel
}
