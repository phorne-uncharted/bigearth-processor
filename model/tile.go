package model

import (
	"io/ioutil"
	"path"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
)

var (
	bandRegex = regexp.MustCompile(`_B[0-9][0-9a-zA-Z][.]`)
)

type Tile struct {
	BaseFolder string
	TileName   string
	Images     []*Image
	Metadata   *TileMetadata
}

type Image struct {
	Band        string
	Filename    string
	ResolutioX  int
	ResolutionY int
}

func NewTile(baseFolder string, tileName string) *Tile {
	return &Tile{
		BaseFolder: baseFolder,
		TileName:   tileName,
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
	log.Infof("load tile found in '%s'", tileFolder)

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

func (t *Tile) LoadImages() error {
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

func (t *Tile) GetCompletePath() string {
	return path.Join(t.BaseFolder, t.TileName)
}

func (i *Image) Load() error {
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
