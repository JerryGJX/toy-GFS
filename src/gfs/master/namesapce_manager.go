package master

import (
	"fmt"
	"gfs"
	"sync"

	log "github.com/sirupsen/logrus"
)

type namespaceManager struct {
	root        *nsTree
	serialIndex int
}

type nsTree struct {
	sync.RWMutex

	// if it is a directory
	isDir    bool
	children map[string]*nsTree

	// if it is a file
	length int64
	chunks int64
}

//for serializing the tree to array and deserializing it back
type serialTreeNode struct {
	isDir    bool
	children map[string]int
	Chunks   int64
}

func (nm *namespaceManager) tree2array(array *[]serialTreeNode, node *nsTree) int {
	n := serialTreeNode{isDir: node.isDir, Chunks: node.chunks}
	if node.isDir {
		n.children = make(map[string]int)
		for k, v := range node.children {
			n.children[k] = nm.tree2array(array, v)
		}
	}
	*array = append(*array, n)
	ret := nm.serialIndex
	nm.serialIndex++
	return ret
}

func (nm *namespaceManager) array2tree(array []serialTreeNode, index int) *nsTree {
	n := &nsTree{
		isDir:  array[index].isDir,
		chunks: array[index].Chunks,
	}
	if array[index].isDir {
		n.children = make(map[string]*nsTree)
		for k, v := range array[index].children {
			n.children[k] = nm.array2tree(array, v)
		}
	}
	return n
}

func (nm *namespaceManager) Serialize() []serialTreeNode {
	nm.root.RLock()
	defer nm.root.RUnlock()
	nm.serialIndex = 0
	var ret []serialTreeNode
	nm.tree2array(&ret, nm.root)
	return ret
}

func (nm *namespaceManager) Deserialize(array []serialTreeNode) error {
	nm.root.Lock()
	defer nm.root.Unlock()
	nm.root = nm.array2tree(array, len(array)-1)
	return nil
}

func (nm *namespaceManager) lockParents(sp *gfs.SplitPath, retSelf bool) (*nsTree, error) {
	ptr := nm.root
	if len(sp.Parts) > 0 {
		for i, part := range sp.Parts[:len(sp.Parts)] {
			ptr.RLock()
			c, ok := ptr.children[part]
			if !ok {
				return ptr, fmt.Errorf("path %s not found", string(sp.SplitPath2Path()))
			}
			if i == len(sp.Parts)-1 {
				if retSelf {
					ptr = c //in order to omit the process of go down in interface
				}
			} else {
				ptr = c
				ptr.RLock()
			}
		}
	}
	return ptr, nil
}

func (nm *namespaceManager) unlockParents(sp *gfs.SplitPath) {
	ptr := nm.root
	if len(sp.Parts) > 0 {
		ptr.RUnlock()
		for _, part := range sp.Parts[:len(sp.Parts)-1] {
			c, ok := ptr.children[part]
			if !ok {
				log.Fatal("[namespace_manager]{unlockParents} error: path not found")
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
			children: make(map[string]*nsTree)},
	}
	log.Info("#################### new namespace manager ####################")
	return nm
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
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

	ptr.children[filename] = new(nsTree)
	return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	sp := p.Path2SplitPath()
	if !sp.IsDir {
		return fmt.Errorf("[namespace_manager]{Mkdir} error: %s is a file", p)
	}
	parent_sp, err := sp.ParentSp()
	if err != nil {
		return err
	}
	filename := sp.Parts[len(sp.Parts)-1]
	ptr, err := nm.lockParents(parent_sp, true)
	defer nm.unlockParents(parent_sp)
	if err != nil {
		return err
	}
	ptr.Lock()
	defer ptr.Unlock()

	if _, ok := ptr.children[filename]; ok {
		return fmt.Errorf("[namespace_manager]{Mkdir} error: %s already exists", p)
	}

	ptr.children[filename] = &nsTree{isDir: true, children: make(map[string]*nsTree)}
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
	log.Fatal("[namespace_manager]{Rename} not implemented")
	return nil
}

func (nm *namespaceManager) List(p gfs.Path) ([]gfs.PathInfo, error) {
	log.Fatal("[namespace_manager]{List} not implemented")
	return nil, nil
}
