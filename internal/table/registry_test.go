package table

import (
	"testing"
)

func TestRegisteredTables_Count(t *testing.T) {
	tables := RegisteredTables()
	if len(tables) != 20 {
		t.Errorf("expected 20 tables, got %d", len(tables))
	}
}

func TestRegisteredTables_HasCoreTables(t *testing.T) {
	names := tableNames(t)
	expected := []string{"chat_card", "chat_card_part", "chat_run", "chat_turn", "inference_session"}
	for _, name := range expected {
		if !contains(names, name) {
			t.Errorf("missing table: %s", name)
		}
	}
}

func TestRegisteredTables_HasThingTables(t *testing.T) {
	names := tableNames(t)
	expected := []string{"thing", "thing_attribute", "thing_attribute_value", "thing_tag", "thing_tag_binding", "thing_category", "thing_scenario"}
	for _, name := range expected {
		if !contains(names, name) {
			t.Errorf("missing table: %s", name)
		}
	}
}

func TestRegisteredTables_InferenceMessageRemoved(t *testing.T) {
	for _, rt := range RegisteredTables() {
		if rt.Table == "inference_message" {
			t.Error("inference_message should not be in registered tables")
		}
	}
}

func TestRegisteredTables_AllHavePublicSchema(t *testing.T) {
	for _, rt := range RegisteredTables() {
		if rt.Schema != "public" {
			t.Errorf("table %s: expected public schema, got %s", rt.Table, rt.Schema)
		}
	}
}

func TestRegisteredTables_AllHaveIDSyncKey(t *testing.T) {
	for _, rt := range RegisteredTables() {
		if len(rt.SyncKeyColumns) != 1 || rt.SyncKeyColumns[0] != "id" {
			t.Errorf("table %s: expected sync key [id], got %v", rt.Table, rt.SyncKeyColumns)
		}
	}
}

func tableNames(t *testing.T) []string {
	t.Helper()
	tables := RegisteredTables()
	names := make([]string, len(tables))
	for i, rt := range tables {
		names[i] = rt.Table
	}
	return names
}

func contains(list []string, item string) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}
