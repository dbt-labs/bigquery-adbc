// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Table-mutation helpers ported from the legacy dbt-labs/arrow-adbc bigquery
// driver. Each helper is dispatched from statement.ExecuteQuery when the
// corresponding statement option is set.

package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// emptyResult returns a record reader with an empty schema/no batches, used
// for statement execution paths that produce no result set.
func emptyResult() (array.RecordReader, int64, error) {
	emptySchema := arrow.NewSchema([]arrow.Field{}, nil)
	reader, err := array.NewRecordReader(emptySchema, []arrow.RecordBatch{})
	if err != nil {
		return nil, -1, err
	}
	return reader, 0, nil
}

// executeAuthorizeViewToDatasets adds a view as an authorized view on one
// or more source datasets, allowing the view to read from them.
//
// The option value is a JSON object: { view_name: [{project, dataset}, ...] }
func (st *statement) executeAuthorizeViewToDatasets(ctx context.Context) (array.RecordReader, int64, error) {
	type dataset struct {
		Project string `json:"project"`
		Dataset string `json:"dataset"`
	}
	var viewToDataset map[string][]dataset
	if err := json.Unmarshal([]byte(st.authorizeViewToDatasets), &viewToDataset); err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] parse authorize_view_to_datasets JSON: %v", err),
		}
	}

	for viewName, datasets := range viewToDataset {
		view, err := stringToTable(st.cnxn.catalog, st.cnxn.dbSchema, viewName)
		if err != nil {
			return nil, -1, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid view name `%s`: %v", viewName, err),
			}
		}
		viewTable := st.cnxn.table(view.ProjectID, view.DatasetID, view.TableID)
		for _, d := range datasets {
			ds := st.cnxn.datasetInProject(d.Project, d.Dataset)
			md, err := ds.Metadata(ctx)
			if err != nil {
				return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "get dataset metadata for %s.%s", d.Project, d.Dataset)
			}
			already := slices.ContainsFunc(md.Access, func(existing *bigquery.AccessEntry) bool {
				return tableEqual(existing.View, viewTable)
			})
			if already {
				continue
			}
			if _, err := ds.Update(ctx, bigquery.DatasetMetadataToUpdate{
				Access: append(md.Access, &bigquery.AccessEntry{View: viewTable, EntityType: bigquery.ViewEntity}),
			}, md.ETag); err != nil {
				return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "update dataset access for %s.%s", d.Project, d.Dataset)
			}
		}
	}
	return emptyResult()
}

func tableEqual(a, b *bigquery.Table) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.TableID == b.TableID && a.DatasetID == b.DatasetID && a.ProjectID == b.ProjectID
}
