package cachefile

import (
	"net/netip"
	"os"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

const fakeipBucketPrefix = "fakeip_"

var (
	bucketFakeIP        = []byte(fakeipBucketPrefix + "address")
	bucketFakeIPDomain4 = []byte(fakeipBucketPrefix + "domain4")
	bucketFakeIPDomain6 = []byte(fakeipBucketPrefix + "domain6")
	keyMetadata         = []byte(fakeipBucketPrefix + "metadata")
)

func (c *CacheFile) FakeIPMetadata() *adapter.FakeIPMetadata {
	var metadata adapter.FakeIPMetadata
	err := c.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketFakeIP)
		if bucket == nil {
			return os.ErrNotExist
		}
		metadataBinary := bucket.Get(keyMetadata)
		if len(metadataBinary) == 0 {
			return os.ErrInvalid
		}
		return metadata.UnmarshalBinary(metadataBinary)
	})
	if err != nil {
		return nil
	}
	return &metadata
}

// FakeIPRangeCompatible permits recovery of legacy caches whose metadata was
// consumed on startup, provided every stored address belongs to today's ranges.
func (c *CacheFile) FakeIPRangeCompatible(inet4Range, inet6Range netip.Prefix) bool {
	compatible := false
	err := c.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketFakeIP)
		if bucket == nil {
			return nil
		}
		compatible = true
		return bucket.ForEach(func(key, value []byte) error {
			if string(key) == string(keyMetadata) {
				return nil
			}
			address, ok := netip.AddrFromSlice(key)
			if !ok || len(value) == 0 || !(inet4Range.Contains(address) || inet6Range.Contains(address)) {
				compatible = false
			}
			return nil
		})
	})
	return err == nil && compatible
}

func (c *CacheFile) FakeIPSaveMetadata(metadata *adapter.FakeIPMetadata) error {
	return c.batch(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketFakeIP)
		if err != nil {
			return err
		}
		metadataBinary, err := metadata.MarshalBinary()
		if err != nil {
			return err
		}
		return bucket.Put(keyMetadata, metadataBinary)
	})
}

func (c *CacheFile) FakeIPSaveMetadataAsync(metadata *adapter.FakeIPMetadata) {
	// Keep the compatibility method durable; delaying this write can persist an
	// obsolete cursor after the corresponding address bindings have been saved.
	_ = c.FakeIPSaveMetadata(metadata)
}

func (c *CacheFile) FakeIPStore(address netip.Addr, domain string) error {
	return c.fakeIPStore(address, domain, nil)
}

// FakeIPStoreWithMetadata commits the new binding and allocation cursor together.
func (c *CacheFile) FakeIPStoreWithMetadata(address netip.Addr, domain string, metadata *adapter.FakeIPMetadata) error {
	return c.fakeIPStore(address, domain, metadata)
}

func (c *CacheFile) fakeIPStore(address netip.Addr, domain string, metadata *adapter.FakeIPMetadata) error {
	return c.batch(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketFakeIP)
		if err != nil {
			return err
		}
		oldDomain := bucket.Get(address.AsSlice())
		err = bucket.Put(address.AsSlice(), []byte(domain))
		if err != nil {
			return err
		}
		if address.Is4() {
			bucket, err = tx.CreateBucketIfNotExists(bucketFakeIPDomain4)
		} else {
			bucket, err = tx.CreateBucketIfNotExists(bucketFakeIPDomain6)
		}
		if err != nil {
			return err
		}
		if oldDomain != nil {
			if err := bucket.Delete(oldDomain); err != nil {
				return err
			}
		}
		if err := bucket.Put([]byte(domain), address.AsSlice()); err != nil {
			return err
		}
		if metadata == nil {
			return nil
		}
		metadataBinary, err := metadata.MarshalBinary()
		if err != nil {
			return err
		}
		return tx.Bucket(bucketFakeIP).Put(keyMetadata, metadataBinary)
	})
}

func (c *CacheFile) FakeIPStoreAsync(address netip.Addr, domain string, logger logger.Logger) {
	c.saveFakeIPAccess.Lock()
	if oldDomain, loaded := c.saveDomain[address]; loaded {
		if address.Is4() {
			delete(c.saveAddress4, oldDomain)
		} else {
			delete(c.saveAddress6, oldDomain)
		}
	}
	c.saveDomain[address] = domain
	if address.Is4() {
		c.saveAddress4[domain] = address
	} else {
		c.saveAddress6[domain] = address
	}
	c.saveFakeIPAccess.Unlock()
	go func() {
		err := c.FakeIPStore(address, domain)
		if err != nil {
			logger.Warn("save FakeIP cache: ", err)
		}
		c.saveFakeIPAccess.Lock()
		delete(c.saveDomain, address)
		if address.Is4() {
			delete(c.saveAddress4, domain)
		} else {
			delete(c.saveAddress6, domain)
		}
		c.saveFakeIPAccess.Unlock()
	}()
}

func (c *CacheFile) FakeIPLoad(address netip.Addr) (string, bool) {
	c.saveFakeIPAccess.RLock()
	cachedDomain, cached := c.saveDomain[address]
	c.saveFakeIPAccess.RUnlock()
	if cached {
		return cachedDomain, true
	}
	var domain string
	_ = c.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketFakeIP)
		if bucket == nil {
			return nil
		}
		domain = string(bucket.Get(address.AsSlice()))
		return nil
	})
	return domain, domain != ""
}

func (c *CacheFile) FakeIPLoadDomain(domain string, isIPv6 bool) (netip.Addr, bool) {
	var (
		cachedAddress netip.Addr
		cached        bool
	)
	c.saveFakeIPAccess.RLock()
	if !isIPv6 {
		cachedAddress, cached = c.saveAddress4[domain]
	} else {
		cachedAddress, cached = c.saveAddress6[domain]
	}
	c.saveFakeIPAccess.RUnlock()
	if cached {
		return cachedAddress, true
	}
	var address netip.Addr
	_ = c.view(func(tx *bbolt.Tx) error {
		var bucket *bbolt.Bucket
		if isIPv6 {
			bucket = tx.Bucket(bucketFakeIPDomain6)
		} else {
			bucket = tx.Bucket(bucketFakeIPDomain4)
		}
		if bucket == nil {
			return nil
		}
		address = M.AddrFromIP(bucket.Get([]byte(domain)))
		return nil
	})
	return address, address.IsValid()
}

func (c *CacheFile) FakeIPReset() error {
	return c.batch(func(tx *bbolt.Tx) error {
		err := tx.DeleteBucket(bucketFakeIP)
		if err != nil {
			return err
		}
		err = tx.DeleteBucket(bucketFakeIPDomain4)
		if err != nil {
			return err
		}
		return tx.DeleteBucket(bucketFakeIPDomain6)
	})
}
