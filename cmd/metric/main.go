package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"runtime"
	"sort"

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
	app.Name = "bigearth-formatter"
	app.Version = "0.1.0"
	app.Usage = "Extract labels from capture metadata and restructure dataset"
	app.UsageText = "bigearth-formatter --metadata-only=<sample> --first-only=<sample> --source=<filepath> --destination=<filepath>"
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
		cli.BoolFlag{
			Name:  "metadata-only",
			Usage: "If true, only the metadata metrics will be tracked",
		},
		cli.IntFlag{
			Name:  "output-frequency",
			Value: 10000,
			Usage: "Output metrics every X tiles",
		},
	}
	app.Action = func(c *cli.Context) error {
		if c.String("source") == "" {
			return cli.NewExitError("missing commandline flag `--source`", 1)
		}

		source := c.String("source")
		firstOnly := c.Bool("first-only")
		metadataOnly := c.Bool("metadata-only")
		outputFrequency := c.Int("output-frequency")

		err := processFolder(source, outputFrequency, metadataOnly, firstOnly)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}

		return nil
	}
	// run app
	app.Run(os.Args)
}

func processFolder(folder string, outputFrequency int, metadataOnly bool, firstOnly bool) error {
	log.Infof("processing folder '%s' (first only: %v, metadata only: %v), outputting metrics every %d", folder, firstOnly, metadataOnly, outputFrequency)
	captures, err := ioutil.ReadDir(folder)
	if err != nil {
		return errors.Wrapf(err, "unable to read contents of '%s'", folder)
	}
	log.Infof("read %d captures", len(captures))

	bandCounts := make(map[string]int)
	labelCounts := make(map[string]int)
	labelSingleCounts := make(map[string]int)
	sizeCounts := make(map[string]int)
	pixelValueCounts := make(map[uint16]int)
	count := 0
	for _, capture := range captures {
		if rand.Float64() < -1 {
			continue
		}

		var tile *model.Tile
		if capture.IsDir() {
			tile = model.NewTile(folder, capture.Name())
		} else {
			tile = model.NewTileMultiBand(path.Join(folder, capture.Name()))
			log.Infof("TILE: %v", tile)
		}

		if metadataOnly {
			log.Infof("loading metadata only")
			tile.LoadMetadata()
		} else {
			log.Infof("loading all files")
			err = tile.LoadFiles()
		}

		if err != nil {
			return err
		}

		for _, img := range tile.Images {
			bandCounts[img.Band]++
			sizeString := fmt.Sprintf("%d X %d", img.SizeX, img.SizeY)
			sizeCounts[sizeString]++
			for _, p := range img.Pixels {
				pixelValueCounts[p]++
			}
		}

		for _, label := range tile.Metadata.Labels {
			labelCounts[label]++
			if len(tile.Metadata.Labels) == 1 {
				labelSingleCounts[label]++
			}
		}

		count++
		if count%10000 == 0 {
			log.Infof("count %d tiles", count)
		}

		if count%outputFrequency == 0 {
			outputMetrics(10000, bandCounts, labelCounts, labelSingleCounts, sizeCounts, pixelValueCounts)
			outputMetrics(40000, bandCounts, labelCounts, labelSingleCounts, sizeCounts, pixelValueCounts)
		}
	}

	outputMetrics(10000, bandCounts, labelCounts, labelSingleCounts, sizeCounts, pixelValueCounts)
	outputMetrics(40000, bandCounts, labelCounts, labelSingleCounts, sizeCounts, pixelValueCounts)

	return nil
}

func outputMetrics(upperLimitPixel uint16, bandCounts map[string]int, labelCounts map[string]int, labelSingleCounts map[string]int, sizeCounts map[string]int, pixelValueCounts map[uint16]int) {
	maxPV := uint16(0)
	minPV := uint16(65535)
	for pv := range pixelValueCounts {
		if pv > maxPV {
			maxPV = pv
		}
		if pv < minPV {
			minPV = pv
		}
	}

	if maxPV > upperLimitPixel {
		maxPV = upperLimitPixel
	}
	pixelCounts := make([]int, (((maxPV+1)/20)+1)*20)
	totalPixelCount := 0
	totalPixelValue := int64(0)
	mostCommonPixelValue := uint16(0)
	mostCommonPixelCount := 0
	for pv, c := range pixelValueCounts {
		if pv > upperLimitPixel {
			pv = upperLimitPixel
		}

		pixelCounts[pv] += c
		totalPixelCount += c
		totalPixelValue += int64(c) * int64(pv)
		if mostCommonPixelCount < c {
			mostCommonPixelCount = c
			mostCommonPixelValue = pv
		}
	}

	medianCount := float64(totalPixelCount) / 2.0
	medianValue := -1.0
	for i := 0; i < len(pixelCounts); i += 20 {
		fmt.Println()
		fmt.Printf("values %d-%d: %v", i, i+19, pixelCounts[i:i+20])
		if medianValue < 0 {
			for j := 0; j < 20; j++ {
				medianCount = medianCount - float64(pixelCounts[i+j])
				if medianCount <= 0 {
					medianValue = float64(i + j)
					break
				} else if medianCount < 1 {
					// assume that every value has at least 1, so median will be between 2 successive values
					medianValue = float64(i+j) + 0.5
					break
				}
			}
		}
	}
	fmt.Println()
	fmt.Printf("total pixel count: %d", totalPixelCount)
	fmt.Println()
	fmt.Printf("total pixel value: %d", totalPixelValue)
	fmt.Println()
	fmt.Printf("most common pixel value: %d", mostCommonPixelValue)
	fmt.Println()
	fmt.Printf("most common pixel count: %d", mostCommonPixelCount)
	fmt.Println()
	fmt.Printf("mean pixel value: %f", float64(totalPixelValue)/float64(totalPixelCount))
	fmt.Println()
	fmt.Printf("median pixel value: %f", medianValue)
	fmt.Println()
	fmt.Printf("min pixel value: %d", minPV)
	fmt.Println()
	fmt.Printf("max pixel value: %d", maxPV)

	for b, c := range bandCounts {
		fmt.Println()
		fmt.Printf("band %s: %d", b, c)
	}

	for s, c := range sizeCounts {
		fmt.Println()
		fmt.Printf("size %s: %d", s, c)
	}

	outputLabels("label", labelCounts)
	outputLabels("label single", labelSingleCounts)
}

func outputLabels(tag string, labelCounts map[string]int) {
	labelResult := make([]*labelCount, 0)
	for l, c := range labelCounts {
		labelResult = append(labelResult, &labelCount{
			label: l,
			count: c,
		})
	}

	sort.Slice(labelResult, func(i int, j int) bool {
		return labelResult[i].count > labelResult[j].count
	})

	for _, lr := range labelResult {
		fmt.Println()
		fmt.Printf("%s %s: %d", tag, lr.label, lr.count)
	}
	fmt.Println()
}
