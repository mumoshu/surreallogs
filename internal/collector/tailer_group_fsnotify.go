package collector

import (
	"fmt"
	"log"

	"github.com/fsnotify/fsnotify"
)

// fsnotifyTailerGroup is a tailerGroup that uses fsnotify to watch for file changes,
// and (re)starts a tailer for each file that is created, modified, and stops it for a deleted file.
type fsnotifyTailerGroup struct {
	watcher *fsnotify.Watcher
	g       TailerGroup
}

func newFsnotifyTailerGroup(g TailerGroup) (*fsnotifyTailerGroup, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}

	fsg, err := &fsnotifyTailerGroup{watcher: watcher, g: g}, nil
	if err != nil {
		return nil, err
	}

	go fsg.start()

	return fsg, nil
}

func (g *fsnotifyTailerGroup) start() {
	for event := range g.watcher.Events {
		g.handleEvent(event)
	}
}

func (g *fsnotifyTailerGroup) handleEvent(event fsnotify.Event) {
	if event.Op&fsnotify.Create == fsnotify.Create {
		log.Printf("New file detected: %s", event.Name)

		if err := g.g.StartTailing(event.Name); err != nil {
			log.Printf("Failed to start collecting logs from %s: %v", event.Name, err)
		}
	} else if event.Op&fsnotify.Rename == fsnotify.Rename {
		// Note that Rename is followed by a Create event.
		//
		// In general, we need to track the inode of the file to know which file is being renamed to which file.
		//
		// In the context of surreallogs, we assume any rename is due to a log rotation,
		// which means updates to the renamed file will eventually stop.
		//
		// But we cannot just ignore the rename event and treat it like a trigger to immediately stop following the log file.
		// Unlike the Remove event, where we can immediately stop following the log file, because it is now gone,
		// the renamed file is still there, and there could be updates we have not yet seen, due to race between the updates, our following, and the rename.
		//
		// So we need to keep following the log file for a while after the rename event.
		log.Printf("File renamed: %s", event.Name)

		if err := g.g.StopTailing(event.Name); err != nil {
			log.Printf("Failed to stop collecting logs from %s: %v", event.Name, err)
		}
	} else if event.Op&fsnotify.Remove == fsnotify.Remove {
		log.Printf("File removed: %s", event.Name)

		if err := g.g.StopTailing(event.Name); err != nil {
			log.Printf("Failed to stop collecting logs from %s: %v", event.Name, err)
		}
	}
}

func (g *fsnotifyTailerGroup) StartTailing(path string) error {
	if err := g.watcher.Add(path); err != nil {
		return nil
	}

	return g.g.StartTailing(path)
}

func (g *fsnotifyTailerGroup) StopTailing(path string) error {
	if err := g.watcher.Remove(path); err != nil {
		return nil
	}

	return g.g.StopTailing(path)
}

func (g *fsnotifyTailerGroup) Close() error {
	if err := g.watcher.Close(); err != nil {
		return err
	}

	return g.g.Close()
}
