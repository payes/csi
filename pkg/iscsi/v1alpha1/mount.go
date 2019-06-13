package iscsi

import (
	apis "github.com/openebs/csi/pkg/apis/openebs.io/core/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"
)

// UnmountAndDetachDisk unmounts the disk from the specified path
// and logs out of the iSCSI Volume
func UnmountAndDetachDisk(vol *apis.CSIVolume, path string) error {
	iscsiInfo := &iscsiDisk{
		VolName: vol.Spec.Volume.Name,
		Portals: []string{vol.Spec.ISCSI.TargetPortal},
		Iqn:     vol.Spec.ISCSI.Iqn,
		lun:     vol.Spec.ISCSI.Lun,
	}

	diskUnmounter := &iscsiDiskUnmounter{
		iscsiDisk: iscsiInfo,
		mounter:   &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: mount.NewOsExec()},
		exec:      mount.NewOsExec(),
	}
	util := &ISCSIUtil{}
	err := util.DetachDisk(*diskUnmounter, path)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

func ResizeVolume(volumePath string) error {
	mounter := mount.New("")
	list, _ := mounter.List()
	for _, mpt := range list {
		if mpt.Path == volumePath {
			util := &ISCSIUtil{}
			if err := util.ReScan(); err != nil {
				return err
			}
			if err := util.ReSize(mpt.Path); err != nil {
				return err
			}
			break
		}
	}
	return nil
}

// AttachAndMountDisk logs in to the iSCSI Volume
// and mounts the disk to the specified path
func AttachAndMountDisk(vol *apis.CSIVolume) (string, error) {
	if len(vol.Spec.Volume.MountPath) == 0 {
		return "", status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	iscsiInfo, err := getISCSIInfo(vol)
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	diskMounter := getISCSIDiskMounter(iscsiInfo, vol)

	util := &ISCSIUtil{}
	devicePath, err := util.AttachDisk(*diskMounter)
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	return devicePath, err
}
