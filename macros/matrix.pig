/*
 * Copyright 2013 Mortar Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

----------------------------------------------------------------------------------------------------

/*
 * All matrices are represented in Sparse COO format:
 * {row: int, col: int, val: float}
 *
 * We use floats instead of doubles because the github recommender does not
 * need numerical precision and it saves space when writing to disk inbetween MR jobs.
 *
 * If you plan to reuse these macros in a context where numerical precision is important,
 * you should change them to use doubles. If numerical precision is extremely important,
 * consider implementing the Kahan Summation Algorithm in a UDF.
 */

REGISTER 'datafu-0.0.10.jar';
DEFINE Matrix__EnumerateFromOne datafu.pig.bags.Enumerate('1');

----------------------------------------------------------------------------------------------------

--
-- Matrix-scalar operations
--

DEFINE Matrix__ScalarProduct(M, mult)
RETURNS out {
    $out    =   FOREACH $M GENERATE row, col, val * $mult AS val;
};

DEFINE Matrix__ElementwisePower(M, pow)
RETURNS out {
    $out    =   FOREACH $M GENERATE 
                    row, col, 
                    (float) org.apache.pig.piggybank.evaluation.math.POW((double) val, $pow) AS val;
};

----------------------------------------------------------------------------------------------------

--
-- Matrix-matrix operations
--

DEFINE Matrix__Sum(A, B)
RETURNS out {
    both    =   UNION A, B;
    $out    =   FOREACH (GROUP both BY (row, col)) GENERATE 
                    FLATTEN(group) AS (row, col),
                    (float) SUM(both.val) AS val;
};

DEFINE Matrix__Product(A, B)
RETURNS product {
    terms       =   FOREACH (JOIN $A BY col, $B BY row) GENERATE
                        $A::row AS row,
                        $B::col AS col,
                        $A::val * $B::val AS val;
    $product    =   FOREACH (GROUP terms BY (row, col)) GENERATE
                        FLATTEN(group) AS (row, col),
                        (float) SUM(terms.val) AS val;
};

DEFINE Matrix__MinPlusProduct(A, B)
RETURNS product {
    terms       =   FOREACH (JOIN $A BY col, $B BY row) GENERATE
                        $A::row AS row,
                        $B::col AS col,
                        $A::val + $B::val AS val;
    $product    =   FOREACH (GROUP terms BY (row, col)) GENERATE
                        FLATTEN(group) AS (row, col),
                        MIN(terms.val) AS val;
};

----------------------------------------------------------------------------------------------------

--
-- Matrix unary operations
--

DEFINE Matrix__Squared(M)
RETURNS m_sq {
    copy    =   FOREACH $M GENERATE *;
    $m_sq   =   Matrix__Product($M, copy);
};

DEFINE Matrix__MinPlusSquared(M)
RETURNS m_sq {
    copy    =   FOREACH $M GENERATE *;
    $m_sq   =   Matrix__MinPlusProduct($M, copy);  
};

DEFINE Matrix__Transpose(M) 
RETURNS m_t {
    $m_t    =   FOREACH $M GENERATE col AS row, row AS col, val;
};

DEFINE Matrix__NormalizeRows(M)
RETURNS normalized {
    with_totals     =   FOREACH (GROUP $M BY row) GENERATE
                            FLATTEN($M) AS (row, col, val), (float) SUM($M.val) AS total;
    $normalized     =   FOREACH with_totals GENERATE
                            row, col, val / total AS val;
};

DEFINE Matrix__NormalizeCols(M)
RETURNS normalized {
    with_totals     =   FOREACH (GROUP $M BY col) GENERATE
                            FLATTEN($M) AS (row, col, val), (float) SUM($M.val) AS total;
    $normalized     =   FOREACH with_totals GENERATE
                            row, col, val / total AS val;
};

DEFINE Matrix__TrimRows(mat, order_direction, max_elems_per_row)
returns trimmed {
    $trimmed        =   FOREACH (GROUP $mat BY row) {
                            ordered = ORDER $mat BY val $order_direction;
                            top     = LIMIT ordered $max_elems_per_row;
                            GENERATE FLATTEN(top) AS (row, col, val);
                        }
};

-- this outputs { row: int, col: int, val: float, rank: int }
DEFINE Matrix__RankRows(mat, order_direction)
RETURNS ranked {
    $ranked         =   FOREACH (GROUP $mat BY row) {
                            ordered = ORDER $mat BY val $order_direction;
                            GENERATE FLATTEN(Matrix__EnumerateFromOne(ordered))
                                     AS (row, col, val, rank);
                        }
};

----------------------------------------------------------------------------------------------------

--
-- Other matrix utilities
--

DEFINE Matrix__NamesToIds(mat)
RETURNS mat_with_ids, id_map {
    row_names_dups  =   FOREACH $mat GENERATE row;
    col_names_dups  =   FOREACH $mat GENERATE col;
    row_names       =   DISTINCT row_ids_dups;
    col_names       =   DISTINCT col_ids_dups;
    all_names       =   UNION row_ids, col_ids;

    enum            =   FOREACH (GROUP all_names ALL) GENERATE
                            FLATTEN(Matrix__EnumerateFromOne(all_names))
                            AS (name, id);
    $id_map         =   FORAECH enum GENERATE id, name;

    join_1          =   FOREACH (JOIN $id_map BY name, $mat BY row) GENERATE
                            id AS row, col AS col, val AS val;
    $mat_with_ids   =   FOREACH (JOIN $id_map BY name, $mat BY col) GENERATE
                            row AS row, id AS col, val AS val;
};

DEFINE Matrix__IdsToNames(mat, id_map)
RETURNS mat_with_names {
    join_1          =   FOREACH (JOIN $id_map BY id, $mat BY col) GENERATE
                            row AS row, $1 AS col, val AS val;
    $mat_with_names =   FOREACH (JOIN $id_map BY id, join_1 BY row) GENERATE
                            $1 AS row, col AS col, val AS val;
};

DEFINE Matrix__VisualizeByRow(mat, id_map, ordering, max_terms_per_row)
RETURNS visualization {
    mat_with_names  =   Matrix__IdsToNames($mat, $id_map);
    $visualization  =   FOREACH (GROUP mat_with_names BY row) {
                            sorted = ORDER mat_with_names BY val $ordering;
                            top    = LIMIT sorted $max_terms_per_row;
                            GENERATE group AS row,
                                     top.(col, val) AS non_zero_terms;
                        }
};
