package com.oath.oak.NVM;

class ActionLogEntry {
    public final int pointer;
    public final int key;

    ActionLogEntry(int pointer, int key) {
        this.pointer = pointer;
        this.key = key;
    }
}
