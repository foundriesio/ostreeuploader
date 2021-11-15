# OSTreeUploader
Tools to upload an ostree repo to the Foundries backend

## Usage

### Build

```make```

### Run

#### Push
Pushes an ostree repo to the Factory storage.
```
./bin/fiopush -creds <credentials.zip> -repo <path-to-repo>
```

#### Check
Checks whether a given ostree repo is present on the Factory storage.
```
./bin/fiocheck -creds <credentials.zip> -repo <path-to-repo>
```

#### Sync
Syncs a given ostree repo commit from one Factory to another.
The `ostree` command line utility must be installed on a host system.
```
./bin/fiosync -src-creds <src-factory_credentials.zip> -dst-creds <dst-factory_credentials.zip> -commit <commit-to-sync> [-repo-dir <directory to download a source repo commit>]
```
If `-repo-dir` is not specified then a temporal directory will be created before the pull process and then removed once a repo is fully synced.
If `-repo-dir` is specified then the repo directory is not removed after the sync process completion.
