package main

import (
	"io/ioutil"
	"os"
	"regexp"
	"runtime"

	"github.com/phorne-uncharted/bigearth-processir/model"
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
		cli.StringFlag{
			Name:  "source",
			Value: "",
			Usage: "The folder containing all big earth captures",
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

		source := c.String("source")
		destination := c.String("destination")
		sample := c.Float64("sample")
		firstOnly := c.Bool("first-only")

		err := processFolder(source, firstOnly)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}

func processFolder(folder string, firstOnly bool) error {
	log.Infof("processing folder '%s' (first only: %v)", folder)
	captures, err := ioutil.ReadDir(folder)
	if err != nil {
		return errors.Wrapf(err, "unable to read contents of '%s'", folder)
	}
	log.Infof("read %d captures", len(captures))

	bandCounts := make(map[string]int)
	for _, capture := range captures {
		tile := model.NewTile(folder, capture.Name())
		tile.LoadMetadata()
		tile.LoadImages()

		for _, img := range tile.Images {
			bandCounts[img.Band]++
		}
	}

	for b, c := range bandCounts {
		log.Infof("band %s: %d", b, c)
	}

	return nil
}
