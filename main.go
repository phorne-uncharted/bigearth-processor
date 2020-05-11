package main

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"regexp"
	"runtime"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"
)

var (
	labelRegex = regexp.MustCompile("[^a-zA-Z0-9]")
)

// CaptureMetadata is the metadata for one set of images from the BigEarth dataset.
type CaptureMetadata struct {
	Labels []string `json:"labels"`
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "bigearth-formatter"
	app.Version = "0.1.0"
	app.Usage = "Extract labels from capture metadata and restructure dataset"
	app.UsageText = "bigearth-formatter --sample=<sample> --source=<filepath> --destination=<filepath>"
	app.Flags = []cli.Flag{
		cli.Float64Flag{
			Name:  "sample",
			Value: 0.0001,
			Usage: "The sample value from 0 to 1",
		},
		cli.StringFlag{
			Name:  "source",
			Value: "",
			Usage: "The folder containing all big earth captures",
		},
		cli.StringFlag{
			Name:  "destination",
			Value: "",
			Usage: "The folder to write the restructured data",
		},
		cli.BoolFlag{
			Name:  "first-only",
			Usage: "If true, only the first label will be used",
		},
	}
	app.Action = func(c *cli.Context) error {
		if c.String("source") == "" {
			return cli.NewExitError("missing commandline flag `--source`", 1)
		}
		if c.String("destination") == "" {
			return cli.NewExitError("missing commandline flag `--destination`", 1)
		}

		source := c.String("source")
		destination := c.String("destination")
		sample := c.Float64("sample")
		firstOnly := c.Bool("first-only")

		err := processFolder(source, destination, sample, firstOnly)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}

func processFolder(folder string, destinationRoot string, sample float64, firstOnly bool) error {
	os.MkdirAll(destinationRoot, os.ModePerm)

	log.Infof("processing folder '%s' with sample rate %f (first only: %v)", folder, sample, firstOnly)
	captures, err := ioutil.ReadDir(folder)
	if err != nil {
		return errors.Wrapf(err, "unable to read contents of '%s'", folder)
	}
	log.Infof("read %d captures", len(captures))

	for _, capture := range captures {
		if rand.Float64() < sample {
			sourceFolder := path.Join(folder, capture.Name())
			labels, err := getLabels(sourceFolder)
			if err != nil {
				return err
			}

			if firstOnly {
				labels = labels[0:1]
			}
			err = copyCapture(sourceFolder, destinationRoot, labels)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func copyCapture(sourceFolder string, destinationRoot string, labels []string) error {
	// metadata is captured in the json file
	files, err := ioutil.ReadDir(sourceFolder)
	if err != nil {
		return errors.Wrapf(err, "unable to read contents of '%s'", sourceFolder)
	}

	labelsExist := make(map[string]bool)
	for _, f := range files {
		fullPath := path.Join(sourceFolder, f.Name())
		if path.Ext(f.Name()) != ".json" {
			data, err := ioutil.ReadFile(fullPath)
			if err != nil {
				return errors.Wrapf(err, "unable to read contents of '%s'", fullPath)
			}
			for _, label := range labels {
				labelCleaned := labelRegex.ReplaceAllString(label, "_")
				if !labelsExist[labelCleaned] {
					os.MkdirAll(path.Join(destinationRoot, labelCleaned), os.ModePerm)
					labelsExist[labelCleaned] = true
				}
				destPath := path.Join(destinationRoot, labelCleaned, f.Name())
				err = ioutil.WriteFile(destPath, data, os.ModePerm)
				if err != nil {
					return errors.Wrapf(err, "unable to write to '%s'", destPath)
				}
			}
		}
	}

	return nil
}

func getLabels(folder string) ([]string, error) {
	// metadata is captured in the json file
	files, err := ioutil.ReadDir(folder)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read contents of '%s'", folder)
	}

	var meta *CaptureMetadata
	for _, f := range files {
		name := path.Join(folder, f.Name())
		if path.Ext(name) == ".json" {
			metadataRaw, err := ioutil.ReadFile(name)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to read metadata from '%s'", name)
			}

			err = json.Unmarshal(metadataRaw, &meta)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to unmarshal metadata from  '%s'", name)
			}
		}
	}

	if meta == nil {
		return nil, errors.Errorf("no metadata found in '%s'", folder)
	}

	return meta.Labels, nil
}
