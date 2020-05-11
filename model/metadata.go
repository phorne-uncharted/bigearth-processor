package model

import (
	"encoding/json"
	"io/ioutil"

	"github.com/pkg/errors"
)

// TileMetadata is the metadata for one set of images from the BigEarth dataset.
type TileMetadata struct {
	Filename string
	Labels   []string `json:"labels"`
}

func NewTileMetadata(filename string) *TileMetadata {
	return &TileMetadata{
		Filename: filename,
	}
}

func (tm *TileMetadata) LoadMetadata() error {
	metadataRaw, err := ioutil.ReadFile(tm.Filename)
	if err != nil {
		return errors.Wrapf(err, "unable to read metadata from '%s'", tm.Filename)
	}

	var labels TileMetadata
	err = json.Unmarshal(metadataRaw, &labels)
	if err != nil {
		return errors.Wrapf(err, "unable to unmarshal metadata from  '%s'", tm.Filename)
	}

	tm.Labels = labels.Labels

	return nil
}
