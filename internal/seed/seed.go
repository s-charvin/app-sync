package seed

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// SystemData copies system data (attribute types, system tags, system
// scenarios, and their bindings/conditions) from template rows (user_id IS NULL)
// into per-user tables. Each INSERT guards with NOT EXISTS so repeated calls
// against the same user are idempotent.
func SystemData(ctx context.Context, tx pgx.Tx, userID string, logger *slog.Logger) error {
	tables := []struct {
		name  string
		query string
	}{
		{name: "thing_attribute", query: `INSERT INTO public.thing_attribute (id, user_id, attribute_type_id, code, description, type_code, is_system, status, name, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, template.attribute_type_id, template.code, template.description, template.type_code, template.is_system, template.status, template.name, $1, now(), now() FROM public.thing_attribute template WHERE template.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_attribute u WHERE u.user_id = $1 AND u.code = template.code)`},
		{name: "thing_tag", query: `INSERT INTO public.thing_tag (id, user_id, code, name, description, icon, color, is_system, status, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, template.code, template.name, template.description, template.icon, template.color, template.is_system, template.status, $1, now(), now() FROM public.thing_tag template WHERE template.user_id IS NULL AND template.is_system = true AND NOT EXISTS (SELECT 1 FROM public.thing_tag u WHERE u.user_id = $1 AND u.code = template.code)`},
		{name: "thing_scenario", query: `INSERT INTO public.thing_scenario (id, user_id, code, name, description, icon, status, priority, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, template.code, template.name, template.description, template.icon, template.status, template.priority, $1, now(), now() FROM public.thing_scenario template WHERE template.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_scenario u WHERE u.user_id = $1 AND u.code = template.code)`},
		{name: "thing_tag_attribute_binding", query: `INSERT INTO public.thing_tag_attribute_binding (id, user_id, tag_id, attribute_id, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, pt.id, pa.id, $1, now(), now() FROM public.thing_tag_attribute_binding tb JOIN public.thing_tag tt ON tt.id = tb.tag_id AND tt.user_id IS NULL JOIN public.thing_tag pt ON pt.code = tt.code AND pt.user_id = $1 JOIN public.thing_attribute ta ON ta.id = tb.attribute_id AND ta.user_id IS NULL JOIN public.thing_attribute pa ON pa.code = ta.code AND pa.user_id = $1 WHERE tb.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_tag_attribute_binding ub WHERE ub.user_id = $1 AND ub.tag_id = pt.id AND ub.attribute_id = pa.id)`},
		{name: "thing_scenario_tag_binding", query: `INSERT INTO public.thing_scenario_tag_binding (id, user_id, scenario_id, tag_id, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, ps.id, pt.id, $1, now(), now() FROM public.thing_scenario_tag_binding sb JOIN public.thing_scenario ts ON ts.id = sb.scenario_id AND ts.user_id IS NULL JOIN public.thing_scenario ps ON ps.code = ts.code AND ps.user_id = $1 JOIN public.thing_tag tt ON tt.id = sb.tag_id AND tt.user_id IS NULL JOIN public.thing_tag pt ON pt.code = tt.code AND pt.user_id = $1 WHERE sb.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_scenario_tag_binding ub WHERE ub.user_id = $1 AND ub.scenario_id = ps.id AND ub.tag_id = pt.id)`},
		{name: "thing_scenario_condition", query: `INSERT INTO public.thing_scenario_condition (id, user_id, scenario_id, expression_ast, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, ps.id, tc.expression_ast, $1, now(), now() FROM public.thing_scenario_condition tc JOIN public.thing_scenario ts ON ts.id = tc.scenario_id AND ts.user_id IS NULL JOIN public.thing_scenario ps ON ps.code = ts.code AND ps.user_id = $1 WHERE tc.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_scenario_condition uc WHERE uc.user_id = $1 AND uc.scenario_id = ps.id)`},
	}

	for _, t := range tables {
		tag, err := tx.Exec(ctx, t.query, userID)
		if err != nil {
			return fmt.Errorf("seed %s: %w", t.name, err)
		}
		logger.Debug("seeded system data", "table", t.name, "rows", tag.RowsAffected())
	}

	logger.Info("seeded system data for user", "user_id", userID)
	return nil
}
