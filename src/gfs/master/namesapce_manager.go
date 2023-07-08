package master

import (
	"fmt"
	"gfs"
	"log"
	"sync"
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
	n := &nsTree{isDir: array[index].isDir, chunks: array[index].Chunks}
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

func (nm *namespaceManager) lockParents(p gfs.Path) (*gfs.SplitPath, *nsTree, error) {
	sp := p.Path2SplitPath()
	ptr := nm.root
	if len(sp.Parts) > 0 {
		for i, part := range sp.Parts[:len(sp.Parts)-1] {
			ptr.RLock()
			c, ok := ptr.children[part]
			if !ok {
				return sp, ptr, fmt.Errorf("path %s not found", p)
			}
			if i == len(sp.Parts)-1 {
				
			} else {
				ptr = c
				ptr.RLock()
			}
		}
	}
	return sp, ptr, nil
}


func (nm *namespaceManager) unlockParents(sp *gfs.SplitPath) {
	ptr := nm.root
	if len(sp.Parts) > 0 {
		ptr.RUnlock()
		for _, part := range sp.Parts[:len(sp.Parts)-1] {
			c, ok := ptr.children[part]
			if !ok {
				log.Fatal("[unlockParents] error: path not found")
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
	return nm
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
	sp := p.Path2SplitPath()

	return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	return nil
}

func (nm *namespaceManager) Delete(p gfs.Path) error {
	return nil
}

func (nm *namespaceManager) Rename(oldPath gfs.Path, newPath gfs.Path) error {
	return nil
}

func (nm *namespaceManager) List(p gfs.Path) ([]gfs.PathInfo, error) {
	return nil, nil
}
