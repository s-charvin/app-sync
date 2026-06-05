package table

import "github.com/mobiletoly/go-oversync/oversync"

// RegisteredTables returns all tables that participate in two-way sync.
// These are the 20 tables from the Android app's PowerSync schema,
// all in the public schema with id as the sync key column.
func RegisteredTables() []oversync.RegisteredTable {
	t := func(name string) oversync.RegisteredTable {
		return oversync.RegisteredTable{Schema: "public", Table: name, SyncKeyColumns: []string{"id"}}
	}
	return []oversync.RegisteredTable{
		t("inference_session"),
		t("chat_run"),
		t("chat_turn"),
		t("chat_card"),
		t("chat_card_part"),

		t("thing_attribute_type"),

		t("thing"),
		t("thing_attribute"),
		t("thing_attribute_value"),
		t("thing_category"),
		t("thing_category_binding"),
		t("thing_scenario"),
		t("thing_scenario_condition"),
		t("thing_scenario_action"),
		t("thing_scenario_execution"),
		t("thing_scenario_tag_binding"),
		t("thing_scenario_trigger"),
		t("thing_tag"),
		t("thing_tag_attribute_binding"),
		t("thing_tag_binding"),
	}
}
