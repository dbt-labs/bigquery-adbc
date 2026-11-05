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

package bigquery

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringToTable(t *testing.T) {
	cnxn := &connectionImpl{catalog: "default-project", dbSchema: "default-dataset"}
	for _, tc := range []struct {
		input   string
		project string
		dataset string
		table   string
	}{
		{"mytable", "default-project", "default-dataset", "mytable"},
		{"mydataset.mytable", "default-project", "mydataset", "mytable"},
		{"myproject.mydataset.mytable", "myproject", "mydataset", "mytable"},
		{"example.com:myproject.mydataset.mytable", "example.com:myproject", "mydataset", "mytable"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := stringToTable(cnxn, tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.project, got.ProjectID)
			require.Equal(t, tc.dataset, got.DatasetID)
			require.Equal(t, tc.table, got.TableID)
		})
	}

	for _, input := range []string{"a.b.c.d", "example.com:myproject.mydataset.mytable.extra"} {
		t.Run(input, func(t *testing.T) {
			_, err := stringToTable(cnxn, input)
			require.Error(t, err)
		})
	}
}
