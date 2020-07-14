package main

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"

	"github.com/phorne-uncharted/bigearth-processor/model"
	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"
)

type labelCount struct {
	label string
	count int
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "bigearth-splitter"
	app.Version = "0.1.0"
	app.Usage = "Split multi band remote sensing into series of single band images"
	app.UsageText = "bigearth-splitter --source=<filepath> --destination=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "source",
			Value: "",
			Usage: "The folder containing all tile images",
		},
		cli.StringFlag{
			Name:  "destination",
			Value: "",
			Usage: "The output folder for the split tiles",
		},
		cli.IntFlag{
			Name:  "log-frequency",
			Value: 500,
			Usage: "Output log every X tiles",
		},
		cli.Float64Flag{
			Name:  "sample",
			Value: 0.0001,
			Usage: "The sample value from 0 to 1",
		},
		cli.StringFlag{
			Name:  "label-data",
			Value: "",
			Usage: "CSV file containing the labels for the tiles",
		},
		cli.StringFlag{
			Name:  "drop-bands",
			Value: "",
			Usage: "CSV list of bands to drop from the image when splitting",
		},
		cli.StringFlag{
			Name:  "band-mapping",
			Value: "",
			Usage: "CSV list of bands to map in the format (old band):(new band)",
		},
		cli.BoolFlag{
			Name:  "split",
			Usage: "If true, multiband image will be split. Otherwise it will be copied.",
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
		labelData := c.String("label-data")
		logFrequency := c.Int("log-frequency")
		bandsToDrop := c.String("drop-bands")
		bandMappingRaw := c.String("band-mapping")
		sample := c.Float64("sample")
		split := c.Bool("split")

		labels, err := loadLabels(labelData)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		bandMapping, err := createBandMapping(bandsToDrop, bandMappingRaw)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		err = processFolder(source, destination, logFrequency, labels, bandMapping, sample, split)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}

func processFolder(inputFolder string, outputFolder string, logFrequency int,
	labelData map[string]string, bandMapping map[int]string, sample float64, split bool) error {
	log.Infof("splitting tiles found in '%s', outputting resulting split images to '%s' (log frequency = %d, sample = %f, split = %v)", inputFolder, outputFolder, logFrequency, sample, split)
	tileFiles, err := ioutil.ReadDir(inputFolder)
	if err != nil {
		return errors.Wrapf(err, "unable to read contents of '%s'", inputFolder)
	}
	log.Infof("read %d tile images", len(tileFiles))

	count := 0
	for _, tileFile := range tileFiles {
		count++
		if count%logFrequency == 0 {
			log.Infof("processed %d tiles", count)
		}

		if rand.Float64() >= sample {
			continue
		}
		if split {
			tile := model.NewTileMultiBand(path.Join(inputFolder, tileFile.Name()))

			err = tile.SplitMultiBand(outputFolder, labelData[tileFile.Name()], bandMapping)
		} else {
			outputFilename := path.Join(outputFolder, labelData[tileFile.Name()], tileFile.Name())
			err = copy(path.Join(inputFolder, tileFile.Name()), outputFilename)
		}

		if err != nil {
			return err
		}
	}

	log.Infof("done splitting tiles")

	return nil
}

func loadLabels(labelFilename string) (map[string]string, error) {
	output := make(map[string]string)
	if labelFilename == "" {
		return output, nil
	}
	log.Infof("reading labels from '%s'", labelFilename)

	// open the file
	csvFile, err := os.Open(labelFilename)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open data file")
	}
	defer csvFile.Close()
	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = 0

	// find the image and label columns
	header, err := reader.Read()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read header from file")
	}
	labelField := -1
	imageField := -1
	for i, f := range header {
		if f == "label" {
			labelField = i
		} else if f == "image" {
			imageField = i
		}
	}
	if labelField == -1 {
		return nil, errors.Errorf("no label field found")
	}
	if imageField == -1 {
		return nil, errors.Errorf("no image field found")
	}

	// read the raw data
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Warnf("failed to read line - %v", err)
			continue
		}

		output[line[imageField]] = line[labelField]
	}

	return output, nil
}

func createBandMapping(bandsToDrop string, bandMappingRaw string) (map[int]string, error) {
	bandMapping := make(map[int]string)

	// bandMappingRaw is a comma separated list of old:new values
	mappingsRaw := strings.Split(bandMappingRaw, ",")
	for _, mr := range mappingsRaw {
		mapping := strings.Split(mr, ":")
		old, err := strconv.Atoi(mapping[0])
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse source band integer for mapping")
		}

		bandMapping[old] = mapping[1]
	}

	// bandsToDrop is a comma separated list of bands
	for _, b := range strings.Split(bandsToDrop, ",") {
		parsed, err := strconv.Atoi(b)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse source band integer to drop")
		}

		bandMapping[parsed] = ""
	}

	// log mapping
	for o, n := range bandMapping {
		log.Infof("mapping band %d to %s", o, n)
	}

	return bandMapping, nil
}

func copy(sourceFile string, destinationFile string) error {
	in, err := os.Open(sourceFile)
	if err != nil {
		return errors.Wrap(err, "unable to open source file")
	}
	defer in.Close()

	// check if the target directory exists and create it if not
	destinationDir := path.Dir(destinationFile)
	if !fileExists(destinationDir) {
		err = os.MkdirAll(destinationDir, os.ModePerm)
		if err != nil {
			return errors.Wrap(err, "unable to make destination folder")
		}
	}

	out, err := os.Create(destinationFile)
	if err != nil {
		return errors.Wrap(err, "unable to create destination file")
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return errors.Wrap(err, "unable to copy file")
	}

	return nil
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}
