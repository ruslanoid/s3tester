package main

import (
	"bytes"
	crand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"time"
	"unsafe"
)

// For performance reasons we need to generate data in blocks as opposed to one character at a time. This is especially true
// for large objects.
//
// This MUST be a power of two to allow for fast modulo optimizations.
// const objectDataBlockSize = 4096
// const objectDataBlockSize = 32 * 1024

// characters for random strings
// var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
// var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// DummyReader implements io.ReadSeeker
type DummyReader struct {
	size   int64
	offset int64
	data   *bytes.Reader
}

func GetDataBlockSize(size int64) int {
	switch {
	case size <= 4*1024:
		return 4096
	case size <= 32*1024:
		return 32 * 1024
	default:
		return 64 * 1024
	}
}

func NewDummyReader(size int64, seed string) *DummyReader {
	// data := generateDataFromKey(seed, GetDataBlockSize(size))

	data := generateDataFromKey(seed, int(size))

	d := DummyReader{size: size, data: bytes.NewReader([]byte(data))}
	return &d
}

func (r *DummyReader) Size() int64 {
	return r.size
}

func (r *DummyReader) Read(p []byte) (n int, err error) {
	dataLength := r.data.Size()

	if dataLength == 0 {
		n, err = 0, errors.New("Data needs to be set before reading")
		return
	}

	if r.offset >= r.size {
		n, err = 0, io.EOF
		return
	}

	bufferLength := len(p)
	read := int(r.size - r.offset)
	if bufferLength < read {
		read = bufferLength
	}

	// This code runs very frequently when doing large object puts so we need to keep it fast and cheap.
	// We try to do that here by reading in blocks and using copy to move larger pieces of memory in a single
	// call as opposed to the naive approach of copying one byte in each iteration.
	bytesTransferred := 0
	for i := 0; i < read; i += bytesTransferred {
		bytesTransferred, _ = r.data.Read(p[i:read])

		if r.data.Len() == 0 {
			r.data.Seek(0, io.SeekStart)
		}
	}

	r.offset += int64(read)

	return read, nil
}

func (r *DummyReader) Seek(offset int64, whence int) (int64, error) {
	updateDummyDataOffset := func() {
		if r.data != nil {
			r.data.Seek(r.offset%r.data.Size(), io.SeekStart)
		}
	}

	switch whence {
	case io.SeekStart:
		if offset >= 0 && offset <= r.size {
			r.offset = offset
			updateDummyDataOffset()
			return r.offset, nil
		}
		return r.offset, errors.New(fmt.Sprintf("SeekStart: Cannot seek past start or end of file. offset: %d, size: %d", offset, r.size))
	case io.SeekCurrent:
		off := offset + r.offset
		if off >= 0 && off <= r.size {
			r.offset = off
			updateDummyDataOffset()
			return off, nil
		}
		return r.offset, errors.New(fmt.Sprintf("SeekCurrent: Cannot seek past start or end of file. offset: %d, size: %d", off, r.size))
	case io.SeekEnd:
		off := r.size - offset
		if off >= 0 && off <= r.size {
			r.offset = off
			updateDummyDataOffset()
			return off, nil
		}
		return r.offset, errors.New(fmt.Sprintf("SeekEnd: Cannot seek past start or end of file. offset: %d, size: %d", off, r.size))
	}
	return 0, errors.New("Invalid value of whence")
}

// We need an efficient way to generate data for objects we write to s3. Ideally
// this data is different for each object. This generates a block of data based
// on the key passed in.
func generateDataFromKey(key string, numBytes int) []byte {
	keylen := len(key)

	if keylen >= numBytes {
		return []byte(key[:numBytes])
	}

	// return []byte(randSeq(numBytes))
	// return []byte(randomString(numBytes))
	return []byte(RandStringBytesMaskImprSrcUnsafe(numBytes))
}

func randSeq(n int) string {
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var letters = []rune(charset)
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[seededRand.Intn(len(letters))]
		// b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func randomString(length int) string {
	// base64 encodes to 6 bits per byte, adding 7 to round up to a full byte.
	data := make([]byte, (length*6+7)/8)

	_, err := crand.Read(data)
	if err != nil {
		panic(err)
	}

	// cut off the tiny bit of extra junk on the end to make the string the requested length.
	return base64.RawURLEncoding.EncodeToString(data)[:length]
}

func RandStringBytesMaskImprSrcUnsafe(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
	b := make([]byte, n)
	src := rand.NewSource(time.Now().UnixNano())
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}
