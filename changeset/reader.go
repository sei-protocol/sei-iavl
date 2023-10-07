package changeset

import (
	"io"
)

type WrapReader struct {
	readerCloser io.ReadCloser
	closer       io.Closer
}

func (r *WrapReader) Read(p []byte) (int, error) {
	return r.readerCloser.Read(p)
}

func (r *WrapReader) ReadByte() (byte, error) {
	var singleByte [1]byte
	_, err := r.readerCloser.Read(singleByte[:])
	return singleByte[0], err
}

func (r *WrapReader) Close() error {
	var errs []error
	if closer, ok := r.readerCloser.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if r.closer != nil {
		err := r.closer.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
