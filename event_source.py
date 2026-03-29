#!/usr/bin/env python3
"""Event sourcing — append-only event log with projections."""
import json, time, sys

class Event:
    def __init__(self, type, data, aggregate_id, version=0):
        self.type = type; self.data = data; self.aggregate_id = aggregate_id
        self.version = version; self.timestamp = time.time()
    def __repr__(self): return f"Event({self.type}, v{self.version})"

class EventStore:
    def __init__(self):
        self.events = []; self.snapshots = {}
    def append(self, event):
        event.version = len([e for e in self.events if e.aggregate_id == event.aggregate_id])
        self.events.append(event); return event
    def get_events(self, aggregate_id, after_version=-1):
        return [e for e in self.events if e.aggregate_id == aggregate_id and e.version > after_version]
    def snapshot(self, aggregate_id, state, version):
        self.snapshots[aggregate_id] = {"state": state, "version": version}

class BankAccount:
    def __init__(self, account_id, store):
        self.id = account_id; self.store = store; self.balance = 0; self.version = -1
    def _apply(self, event):
        if event.type == "deposited": self.balance += event.data["amount"]
        elif event.type == "withdrawn": self.balance -= event.data["amount"]
        self.version = event.version
    def deposit(self, amount):
        e = self.store.append(Event("deposited", {"amount": amount}, self.id))
        self._apply(e)
    def withdraw(self, amount):
        if amount > self.balance: raise ValueError("Insufficient funds")
        e = self.store.append(Event("withdrawn", {"amount": amount}, self.id))
        self._apply(e)
    def rebuild(self):
        self.balance = 0; self.version = -1
        snap = self.store.snapshots.get(self.id)
        if snap: self.balance = snap["state"]["balance"]; self.version = snap["version"]
        for e in self.store.get_events(self.id, self.version): self._apply(e)

if __name__ == "__main__":
    store = EventStore()
    acc = BankAccount("ACC-001", store)
    acc.deposit(1000); acc.deposit(500); acc.withdraw(200)
    print(f"Balance: ${acc.balance}")
    print(f"Events: {store.get_events('ACC-001')}")
    store.snapshot("ACC-001", {"balance": acc.balance}, acc.version)
    acc.deposit(100)
    acc2 = BankAccount("ACC-001", store)
    acc2.rebuild()
    print(f"Rebuilt from snapshot: ${acc2.balance}")
