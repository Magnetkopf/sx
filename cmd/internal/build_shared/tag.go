package build_shared

import (
	"github.com/sagernet/sing-box/common/badversion"
	"github.com/sagernet/sing/common/shell"
)

func ReadTag() (string, error) {
	currentTag, err := shell.Exec("git", "describe", "--tags").ReadOutput()
	if err != nil {
		shortCommit, err2 := shell.Exec("git", "rev-parse", "--short", "HEAD").ReadOutput()
		if err2 != nil {
			return "", err
		}
		return "0.0.0-dev-" + shortCommit, nil
	}
	currentTagRev, _ := shell.Exec("git", "describe", "--tags", "--abbrev=0").ReadOutput()
	if currentTagRev == currentTag {
		return currentTag[1:], nil
	}
	shortCommit, _ := shell.Exec("git", "rev-parse", "--short", "HEAD").ReadOutput()
	version := badversion.Parse(currentTagRev[1:])
	return version.String() + "-" + shortCommit, nil
}

func ReadTagVersionRev() (badversion.Version, error) {
	currentTagRev, err := shell.Exec("git", "describe", "--tags", "--abbrev=0").ReadOutput()
	if err != nil {
		return badversion.Version{PreReleaseIdentifier: "dev"}, nil
	}
	return badversion.Parse(currentTagRev[1:]), nil
}

func ReadTagVersion() (badversion.Version, error) {
	currentTag, err := shell.Exec("git", "describe", "--tags").ReadOutput()
	if err != nil {
		return badversion.Version{PreReleaseIdentifier: "dev"}, nil
	}
	currentTagRev, err := shell.Exec("git", "describe", "--tags", "--abbrev=0").ReadOutput()
	if err != nil {
		return badversion.Version{PreReleaseIdentifier: "dev"}, nil
	}
	version := badversion.Parse(currentTagRev[1:])
	if currentTagRev != currentTag {
		if version.PreReleaseIdentifier == "" {
			version.Patch++
		}
	}
	return version, nil
}
