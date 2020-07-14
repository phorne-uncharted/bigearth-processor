package main

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"regexp"
	"runtime"

	"github.com/phorne-uncharted/bigearth-processor/model"
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
		cli.BoolFlag{
			Name:  "single-only",
			Usage: "If true, only consider tiles with one label",
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
		singleOnly := c.Bool("single-only")

		err := processFolder(source, destination, sample, firstOnly, singleOnly)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}

func processFolder(folder string, destinationRoot string, sample float64, firstOnly bool, singleOnly bool) error {
	os.MkdirAll(destinationRoot, os.ModePerm)

	log.Infof("processing folder '%s' with sample rate %f (first only: %v, single only: %v)", folder, sample, firstOnly, singleOnly)
	captures, err := ioutil.ReadDir(folder)
	if err != nil {
		return errors.Wrapf(err, "unable to read contents of '%s'", folder)
	}
	log.Infof("read %d captures", len(captures))

	count := 0
	for _, capture := range captures {
		if rand.Float64() < sample {
			tile := model.NewTile(folder, capture.Name())
			err = tile.LoadMetadata()
			if err != nil {
				return err
			}

			labels := tile.Metadata.Labels
			if singleOnly && len(labels) != 1 {
				continue
			}

			if firstOnly {
				labels = labels[0:1]
			}
			err = copyCapture(path.Join(folder, capture.Name()), destinationRoot, labels)
			if err != nil {
				return err
			}

			count++
			if count%10000 == 0 {
				log.Infof("processed %d", count)
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
