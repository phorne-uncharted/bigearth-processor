package model

import (
	"bytes"
	"fmt"
	"image"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"

	"golang.org/x/image/tiff"

	"github.com/pkg/errors"
	"github.com/uncharted-distil/gdal"
)

var (
	bandRegex = regexp.MustCompile(`_B[0-9][0-9a-zA-Z][.]`)
)

type Tile struct {
	BaseFolder string
	TileName   string
	Images     []*Image
	Metadata   *TileMetadata
	MultiBand  bool
}

type Image struct {
	Band     string
	Filename string
	SizeX    int
	SizeY    int
	Pixels   []uint16
}

func NewTile(baseFolder string, tileName string) *Tile {
	return &Tile{
		BaseFolder: baseFolder,
		TileName:   tileName,
		MultiBand:  false,
	}
}

func NewTileMultiBand(imageName string) *Tile {
	return &Tile{
		BaseFolder: path.Dir(imageName),
		TileName:   path.Base(imageName),
		MultiBand:  true,
	}
}

func NewImage(filename string) *Image {
	band := extractBand(filename)

	return &Image{
		Band:     band,
		Filename: filename,
	}
}

func (t *Tile) LoadMetadata() error {
	// read the folder from the tile name
	tileFolder := t.GetCompletePath()

	// read the files in the tile folder
	imageFiles, err := ioutil.ReadDir(tileFolder)
	if err != nil {
		return errors.Wrapf(err, "unable to read contents of '%s'", tileFolder)
	}

	// cycle through files to find the metadata file
	t.Images = make([]*Image, 0)
	for _, f := range imageFiles {
		if path.Ext(f.Name()) == ".json" {
			t.Metadata = NewTileMetadata(path.Join(tileFolder, f.Name()))
			t.Metadata.LoadMetadata()
			break
		}
	}

	return nil
}

func (t *Tile) LoadFiles() error {
	if t.MultiBand {
		return t.LoadImages()
	}

	// read the folder from the tile name
	tileFolder := t.GetCompletePath()

	// read the files in the tile folder
	imageFiles, err := ioutil.ReadDir(tileFolder)
	if err != nil {
		return errors.Wrapf(err, "unable to read contents of '%s'", tileFolder)
	}

	// cycle through files, opening them and getting the resolutions and bands
	t.Images = make([]*Image, 0)
	for _, f := range imageFiles {
		if path.Ext(f.Name()) == ".json" {
			t.Metadata = NewTileMetadata(path.Join(tileFolder, f.Name()))
			t.Metadata.LoadMetadata()
		} else {
			imagePath := path.Join(tileFolder, f.Name())
			img := NewImage(imagePath)
			err = img.Load()
			if err != nil {
				return errors.Wrapf(err, "unable to load image from '%s'", imagePath)
			}

			t.Images = append(t.Images, img)
		}
	}

	return nil
}

func (t *Tile) LoadImages() error {
	if t.MultiBand {
		return t.loadMultiBandImage()
	}

	return t.loadSingleBandImages()
}

func (t *Tile) GetCompletePath() string {
	return path.Join(t.BaseFolder, t.TileName)
}

func (i *Image) Load() error {

	data, err := ioutil.ReadFile(i.Filename)
	if err != nil {
		return errors.Wrap(err, "unable to read raw image")
	}

	im, err := tiff.Decode(bytes.NewBuffer(data))
	if err != nil {
		return errors.Wrap(err, "unable to decode tiff image")
	}
	imGray := im.(*image.Gray16)
	pixelsRaw := imGray.Pix

	i.SizeX = imGray.Rect.Max.X - imGray.Rect.Min.X
	i.SizeY = imGray.Rect.Max.Y - imGray.Rect.Min.Y

	pixels := make([]uint16, i.SizeX*i.SizeY)
	for x := 0; x < len(pixels); x++ {
		pixels[x] = uint16(pixelsRaw[x*2])<<8 | uint16(pixelsRaw[x*2+1])
	}
	i.Pixels = pixels

	return nil
}

func extractBand(filename string) string {
	bandRaw := bandRegex.Find([]byte(filename))
	if len(bandRaw) > 0 {
		band := string(bandRaw)
		return strings.ToLower(band[2 : len(band)-1])
	}

	return ""
}

func (t *Tile) loadMultiBandImage() error {
	filename := path.Join(t.BaseFolder, t.TileName)

	dataset, err := gdal.Open(filename, gdal.ReadOnly)
	if err != nil {
		return errors.Wrapf(err, "unable to load geotiff")
	}
	defer dataset.Close()

	t.SplitMultiBand("C:\\data\\remote-sensing\\test1", "", map[int]string{})

	return nil
}

func (t *Tile) SplitMultiBand(outputFolder string, label string, bandMapping map[int]string) error {
	os.MkdirAll(outputFolder, os.ModePerm)
	// load the multiband image
	filename := path.Join(t.BaseFolder, t.TileName)

	dataset, err := gdal.Open(filename, gdal.ReadOnly)
	if err != nil {
		return errors.Wrapf(err, "unable to load geotiff")
	}
	defer dataset.Close()

	tileName := t.TileName
	tileName = strings.TrimSuffix(tileName, path.Ext(tileName))
	var folderName string
	if label == "" {
		folderName = path.Join(outputFolder, tileName)
	} else {
		folderName = path.Join(outputFolder, label)
	}

	os.MkdirAll(folderName, os.ModePerm)

	for band := 1; band <= dataset.RasterCount(); band++ {
		mappedBand, ok := bandMapping[band]
		if ok && mappedBand == "" {
			continue
		} else if !ok {
			mappedBand = fmt.Sprintf("%02d", band)
		}

		name := fmt.Sprintf("%s_B%s.tiff", tileName, mappedBand)
		dst := gdal.GDALTranslate(path.Join(folderName, name), dataset, []string{"-b", fmt.Sprintf("%d", band)})
		dst.Close()
	}

	return nil
}

func (t *Tile) loadSingleBandImages() error {
	// read the folder from the tile name
	tileFolder := t.GetCompletePath()

	// read the files in the tile folder
	imageFiles, err := ioutil.ReadDir(tileFolder)
	if err != nil {
		return errors.Wrapf(err, "unable to read contents of '%s'", tileFolder)
	}

	// cycle through files, opening them and getting the resolutions and bands
	t.Images = make([]*Image, 0)
	for _, f := range imageFiles {
		if path.Ext(f.Name()) != ".json" {
			imagePath := path.Join(tileFolder, f.Name())
			img := NewImage(imagePath)

			err = img.Load()
			if err != nil {
				return errors.Wrapf(err, "unable to load image from '%s'", imagePath)
			}

			t.Images = append(t.Images, img)
		}
	}

	return nil
}
