package master

import (
	"fmt"
	"gfs"

	// "encoding/json"

	// "sync"
	sync "github.com/sasha-s/go-deadlock"
	log "github.com/sirupsen/logrus"
)

type namespaceManager struct {
	root        *nsTree
	serialIndex int
}

type nsTree struct {
	sync.RWMutex

	AbsPath gfs.Path

	// if it is a directory
	isDir    bool
	children map[string]*nsTree

	// if it is a file
	length int64
	chunks int64
}

// for serializing the tree to array and deserializing it back
type serialTreeNode struct {
	IsDir bool

	AbsPath gfs.Path

	Children map[string]int
	Chunks   int64
}

func (nm *namespaceManager) tree2array(array *[]serialTreeNode, node *nsTree) int {
	n := serialTreeNode{IsDir: node.isDir, Chunks: node.chunks, AbsPath: node.AbsPath}
	if node.isDir {
		n.Children = make(map[string]int)
		for k, v := range node.children {
			n.Children[k] = nm.tree2array(array, v)
		}
	}
	*array = append(*array, n)
	ret := nm.serialIndex
	nm.serialIndex++
	return ret
}

func (nm *namespaceManager) array2tree(array []serialTreeNode, index int) *nsTree {
	n := &nsTree{
		isDir:   array[index].IsDir,
		chunks:  array[index].Chunks,
		AbsPath: array[index].AbsPath,
	}
	if array[index].IsDir {
		n.children = make(map[string]*nsTree)
		for k, v := range array[index].Children {
			n.children[k] = nm.array2tree(array, v)
		}
	}
	return n
}

func (nm *namespaceManager) Serialize(rootPath gfs.Path) []serialTreeNode { //for snapshot and checkpoint
	sp := rootPath.Path2SplitPath()
	cwd, err := nm.lockParents(sp, true)
	defer nm.unlockParents(sp)

	if err != nil {
		log.Fatal("[namespace_manager]{Serialize} error: ", err)
	}
	cwd.RLock()
	defer cwd.RUnlock() //to check

	nm.serialIndex = 0
	var ret []serialTreeNode
	nm.tree2array(&ret, cwd)
	// log.Info("[namespace_manager]{Serialize} serialized data: ", ret)
	// json_data, err := json.Marshal(ret)
	// if err != nil {
	// 	log.Fatal("[namespace_manager]{Serialize} error: ", err)
	// }
	// log.Info("[namespace_manager]{Serialize} json data: ", string(json_data))
	return ret
}

func (nm *namespaceManager) CheckpointRecovery(array []serialTreeNode) error { //for recovery only
	nm.root.Lock()
	defer nm.root.Unlock()
	nm.root = nm.Deserialize(array)
	// log.Info("[namespace_manager]{Deserialize} print children of root: ", nm.root.children)
	// log.Info("[namespace_manager]{Deserialize} raw data: ", array)
	return nil
}

func (nm *namespaceManager) Deserialize(array []serialTreeNode) *nsTree { // to deserialize the snapshot
	ret := nm.array2tree(array, len(array)-1)
	// log.Info("[namespace_manager]{Deserialize} print children of root: ", nm.root.children)
	// log.Info("[namespace_manager]{Deserialize} raw data: ", array)
	return ret
}

func (nm *namespaceManager) lockParents(sp *gfs.SplitPath, retSelf bool) (*nsTree, error) {
	ptr := nm.root
	// log.Info("[namespace_manager]{lockParents} enter, path: ", sp, "; part.num: ", len(sp.Parts))
	// log.Info("[namespace_manager]{lockParents} print children of root: ", nm.root.children)
	if len(sp.Parts) > 0 {
		// log.Info("[namespace_manager]{lockParents} before lock ptr: ", ptr.name)
		ptr.RLock()
		for i, part := range sp.Parts[:len(sp.Parts)] {
			// log.Info("[namespace_manager]{lockParents} lock ", ptr.name, "; i = ", i)
			c, ok := ptr.children[part]
			if !ok {
				log.Warningf("[namespace_manager]{lockParents} part = %v not found", part)
				return ptr, fmt.Errorf("[namespaceManager]{lockParents} path %s not found", string(sp.SplitPath2Path()))
			}
			if i == len(sp.Parts)-1 { //the last
				if retSelf {
					ptr = c
					//in order to omit the process of go down in interface
				}
			} else {
				ptr = c

				// log.Info("[namespace_manager]{lockParents} before lock ptr: ", ptr.name)
				ptr.RLock()
			}
		}
	}
	return ptr, nil
}

func (nm *namespaceManager) unlockParents(sp *gfs.SplitPath) {
	ptr := nm.root
	// log.Info("[namespace_manager]{unlockParents} sp= ", sp, "; element num = ", len(sp.Parts))
	if len(sp.Parts) > 0 {
		ptr.RUnlock()
		for _, part := range sp.Parts[:len(sp.Parts)-1] {
			// log.Info("[namespace_manager]{unlockParents} unlock ", part)
			c, ok := ptr.children[part]
			if !ok {
				log.Fatal("[namespace_manager]{unlockParents} error: path not found, path: ", string(sp.SplitPath2Path()))
				return
			}
			ptr = c
			ptr.RUnlock()
		}
	}
}

func newNamespaceManager() *namespaceManager {
	nm := &namespaceManager{
		root: &nsTree{isDir: true,
			children: make(map[string]*nsTree), AbsPath: "/"},
	}
	// log.Info("#################### new namespace manager ####################")
	return nm
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
	// log.Info("[namespace_manager]{Create} enter")
	sp := p.Path2SplitPath()
	if sp.IsDir {
		return fmt.Errorf("[namespace_manager]{Create} error: %s is a directory", p)
	}
	filename := sp.Parts[len(sp.Parts)-1]
	parent_sp, err := sp.ParentSp()
	if err != nil {
		return err
	}
	ptr, err := nm.lockParents(parent_sp, true)
	defer nm.unlockParents(parent_sp)
	if err != nil {
		return err
	}
	ptr.Lock()
	defer ptr.Unlock()

	if _, ok := ptr.children[filename]; ok {
		return fmt.Errorf("[namespace_manager]{Create} error: %s already exists", p)
	}

	ptr.children[filename] = &nsTree{isDir: false, length: 0, chunks: 0, AbsPath: p}
	return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	// log.Info("[namespace_manager]{Mkdir} enter, path: ", p)
	sp := p.Path2SplitPath()
	if !sp.IsDir {
		return fmt.Errorf("[namespace_manager]{Mkdir} error: %s is a file", p)
	}
	parent_sp, err := sp.ParentSp()
	// log.Info("[namespace_manager]{Mkdir} parent_sp: ", parent_sp)
	if err != nil {
		return err
	}
	dirname := sp.Parts[len(sp.Parts)-1]
	ptr, err := nm.lockParents(parent_sp, true)
	defer nm.unlockParents(parent_sp)
	if err != nil {
		return err
	}
	ptr.Lock()
	defer ptr.Unlock()

	if _, ok := ptr.children[dirname]; ok {
		return fmt.Errorf("[namespace_manager]{Mkdir} error: %s already exists", p)
	}

	ptr.children[dirname] = &nsTree{isDir: true, children: make(map[string]*nsTree), AbsPath: p}
	return nil
}

func (nm *namespaceManager) Delete(p gfs.Path) error { //diff
	sp := p.Path2SplitPath()
	if sp.IsDir {
		return fmt.Errorf("[namespace_manager]{Create} error: %s is a directory", p)
	}
	filename := sp.Parts[len(sp.Parts)-1]
	parent_sp, err := sp.ParentSp()
	if err != nil {
		return err
	}
	ptr, err := nm.lockParents(parent_sp, true)
	defer nm.unlockParents(parent_sp)
	if err != nil {
		return err
	}
	ptr.Lock()
	defer ptr.Unlock()

	//rename the file to be deleted
	target := ptr.children[filename]
	delete(ptr.children, filename)
	ptr.children[gfs.DeletedFilePrefix+filename] = target

	return nil
}

func (nm *namespaceManager) Rename(oldPath gfs.Path, newPath gfs.Path) error {
	// log.Fatal("[namespace_manager]{Rename} not implemented")
	return nil
}

func (nm *namespaceManager) List(p gfs.Path) ([]gfs.PathInfo, error) {
	// log.Info("[namespace_manager]{List} enter")
	sp := p.Path2SplitPath()
	if !sp.IsDir {
		return nil, fmt.Errorf("[namespace_manager]{list} path: ", p, " not a directory")
	}
	var dir *nsTree
	if p == gfs.Path("/") {
		dir = nm.root
	} else {
		cwd, err := nm.lockParents(sp, true)
		defer nm.unlockParents(sp)
		if err != nil {
			return nil, err
		}
		dir = cwd
	}
	dir.RLock()
	defer dir.RUnlock()

	ls := make([]gfs.PathInfo, 0, len(dir.children))
	for name, val := range dir.children {
		item := gfs.PathInfo{
			Name:   name,
			IsDir:  val.isDir,
			Length: val.length,
			Chunks: val.chunks,
		}
		ls = append(ls, item)
	}
	return ls, nil
}

// for snapshot
func (mn *namespaceManager) listFile(cwd *nsTree) ([]gfs.Path, error) { // the caller will lock the node
	var ret []gfs.Path
	for _, val := range cwd.children {
		if val.isDir {
			val.RLock()
			tmp, err := mn.listFile(val)
			val.RUnlock()
			if err != nil {
				return nil, err
			}
			ret = append(ret, tmp...)
		} else {
			ret = append(ret, val.AbsPath)
		}
	}
	return ret, nil
}

func (mn *namespaceManager) ListRelatedFile(p gfs.Path) ([]gfs.Path, error) {
	sp := p.Path2SplitPath()
	cwd, err := mn.lockParents(sp, true)
	defer mn.unlockParents(sp)
	if err != nil {
		return nil, err
	}
	cwd.RLock()
	defer cwd.RUnlock()

	return mn.listFile(cwd)
}
