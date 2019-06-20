package utils

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/container-storage-interface/spec/lib/go/csi"
	apismaya "github.com/openebs/csi/pkg/apis/openebs.io/maya/v1alpha1"
	errors "github.com/openebs/csi/pkg/generated/maya/errors/v1alpha1"
)

const (
	kib    int64 = 1024
	mib    int64 = kib * 1024
	gib    int64 = mib * 1024
	gib100 int64 = gib * 100
	tib    int64 = gib * 1024
	tib100 int64 = tib * 100
)

// TODO
//  Need to remove the dependency of maya api server
// Provisioning workflow should be tightly integrated
// with Kubernetes based custom resources. This tight
// integration helps in decoupling two or more
// applications.
//
// ProvisionVolume sends a request to maya
// apiserver to create a new CAS volume
func ProvisionVolume(req *csi.CreateVolumeRequest) (*apismaya.CASVolume, error) {
	casVolume := apismaya.CASVolume{}
	casVolume.Spec.Capacity = strconv.FormatInt(req.GetCapacityRange().GetRequiredBytes(), 10)

	parameters := req.GetParameters()
	storageclass := parameters["storageclass"]
	namespace := parameters["namespace"]

	// creating a map b/c have to initialize the map
	// using the make function before adding any elements
	// to avoid nil map assignment error
	mapLabels := make(map[string]string)

	if storageclass == "" {
		logrus.Errorf("volume is not specified with storageclass")
	} else {
		mapLabels[string(apismaya.StorageClassKey)] = storageclass
		casVolume.Labels = mapLabels
	}

	casVolume.Labels[string(apismaya.NamespaceKey)] = namespace
	casVolume.Namespace = namespace
	casVolume.Labels[string(apismaya.PersistentVolumeClaimKey)] =
		parameters["persistentvolumeclaim"]
	casVolume.Name = req.GetName()

	logrus.Infof("verify if volume {%s} is already present", casVolume.Name)
	err := ReadVolume(req.GetName(), namespace, storageclass, &casVolume)
	if err == nil {
		logrus.Infof("volume {%v} already present", req.GetName())
		return &casVolume, nil
	}

	if err.Error() != http.StatusText(404) {
		// any error other than 404 is unexpected error
		logrus.Errorf("failed to read volume {%s}: %v", req.GetName(), err)
		return nil, err
	}

	if err.Error() == http.StatusText(404) {
		logrus.Infof("volume {%s} does not exist: will attempt to create", req.GetName())

		err = CreateVolume(casVolume)
		if err != nil {
			logrus.Errorf(
				"failed to create volume {%s}: %v",
				req.GetName(),
				err)
			return nil, err
		}

		err = ReadVolume(req.GetName(), namespace, storageclass, &casVolume)
		if err != nil {
			logrus.Errorf("failed to read volume {%s}: %v", req.GetName(), err)
			return nil, err
		}

		logrus.Infof("volume {%s} created successfully", req.GetName())
	}

	return &casVolume, nil
}

func requestMAPIServer(reqType, url, namespace, storageclass string, obj interface{}, jsonValue []byte) error {
	var retried bool
connect:
	req, err := http.NewRequest(reqType, url, bytes.NewBuffer(jsonValue))
	if err != nil {
		return err
	}

	if namespace != "" {
		req.Header.Set("namespace", namespace)
	}
	// passing storageclass info as a request header which will extracted by the
	// Maya-apiserver to get the CAS template name
	if storageclass != "" {
		req.Header.Set(string(apismaya.StorageClassHeaderKey), storageclass)
	}
	c := &http.Client{
		Timeout: timeout,
	}
	resp, err := c.Do(req)
	if err != nil {
		logrus.Errorf("Error when connecting to maya-apiserver %v", err)
		if !retried {
			retried = true
			updateMAPIServerEndPoint()
			goto connect
		}
		return err
	}
	defer resp.Body.Close()

	code := resp.StatusCode
	if code != http.StatusOK {
		logrus.Errorf("HTTP Status error from maya-apiserver: %v\n",
			http.StatusText(code))
		return errors.New(http.StatusText(code))
	}
	if obj != nil {
		return json.NewDecoder(resp.Body).Decode(obj)
	}
	return nil
}

// ReadVolume to get the info of CAS volume through a API call to m-apiserver
func ReadVolume(vname, namespace, storageclass string, obj interface{}) error {
	url := MAPIServerEndpoint + "/latest/volumes/" + vname
	return requestMAPIServer("GET", url, namespace, storageclass, obj, nil)
}

// DeleteVolume deletes CAS volume through an
// API call to maya apiserver
func DeleteVolume(name, namespace string) error {
	url := MAPIServerEndpoint + "/latest/volumes/" + name
	return requestMAPIServer("DELETE", url, namespace, "", "", nil)
}

// CreateVolume creates the CAS volume through
// an API call to maya apiserver
func CreateVolume(vol apismaya.CASVolume) error {

	url := MAPIServerEndpoint + "/latest/volumes/"
	// Marshal serializes the value provided into a json document
	jsonValue, _ := json.Marshal(vol)
	return requestMAPIServer("POST", url, "", "", "", jsonValue)
}
