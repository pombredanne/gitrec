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
 * We use floats instead of doubles because the our use cases so far have not needed
 * numerical precision and it saves space when writing to disk inbetween MR jobs.
 *
 * If you do need numerical precision, consider reimplementing these with doubles
 * or even implementing the Kahan Summation Algorithm instead of using the builtin SUM function.
 */

REGISTER 'datafu-0.0.10.jar';
DEFINE Matrix__Enumerate datafu.pig.bags.Enumerate('1');

----------------------------------------------------------------------------------------------------

--
-- Matrix-scalar operations
--

/*
 * M: {row: int, col: int, val: float}
 * mult: float
 * -->
 * out: {row: int, col: int, val: float}
 */
DEFINE Matrix__ScalarProduct(M, mult)
RETURNS out {
    $out    =   FOREACH $M GENERATE row, col, val * $mult AS val;
};

/*
 * M: {row: int, col: int, val: float}
 * pow: float or double
 * -->
 * out: {row: int, col: int, val: float}
 */
DEFINE Matrix__ElementwisePower(M, pow)
RETURNS out {
    $out    =   FOREACH $M GENERATE 
                    row, col, 
                    (float) org.apache.pig.piggybank.evaluation.math.POW((double) val, (double) $pow) AS val;
};

----------------------------------------------------------------------------------------------------

--
-- Matrix-matrix operations
--

/*
 * A: {row: int, col: int, val: float}
 * B: {row: int, col: int, val: float}
 * -->
 * out: {row: int, col: int, val: float}
 */
DEFINE Matrix__Sum(A, B)
RETURNS sum {
    both    =   UNION A, B;
    $sum    =   FOREACH (GROUP both BY (row, col)) GENERATE 
                    FLATTEN(group) AS (row, col),
                    (float) SUM(both.val) AS val;
};

/*
 * A: {row: int, col: int, val: float}
 * B: {row: int, col: int, val: float}
 * -->
 * product: {row: int, col: int, val: float}
 */
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

/*
 * A: {row: int, col: int, val: float}
 * B: {row: int, col: int, val: float}
 * -->
 * product: {row: int, col: int, val: float}
 */
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

/*
 * M: {row: int, col: int, val: float}
 * -->
 * M_sq: {row: int, col: int, val: float}
 */
DEFINE Matrix__Squared(M)
RETURNS M_sq {
    copy    =   FOREACH $M GENERATE *;
    $M_sq   =   Matrix__Product($M, copy);
};

/*
 * M: {row: int, col: int, val: float}
 * -->
 * M_sq: {row: int, col: int, val: float}
 */
DEFINE Matrix__MinPlusSquared(M)
RETURNS M_sq {
    copy    =   FOREACH $M GENERATE *;
    $M_sq   =   Matrix__MinPlusProduct($M, copy);  
};

/*
 * M: {row: int, col: int, val: float}
 * -->
 * M_t: {row: int, col: int, val: float}
 */
DEFINE Matrix__Transpose(M) 
RETURNS M_t {
    $M_t    =   FOREACH $M GENERATE col AS row, row AS col, val;
};

/*
 * Normalizes each row so that the values sum to 1
 *
 * M: {row: int, col: int, val: float}
 * -->
 * normalized: {row: int, col: int, val: float}
 */
DEFINE Matrix__NormalizeRows(M)
RETURNS normalized {
    with_totals     =   FOREACH (GROUP $M BY row) GENERATE
                            FLATTEN($M) AS (row, col, val), (float) SUM($M.val) AS total;
    $normalized     =   FOREACH with_totals GENERATE
                            row, col, val / total AS val;
};

/*
 * Normalizes each column so that the values sum to 1
 *
 * M: {row: int, col: int, val: float}
 * -->
 * normalized: {row: int, col: int, val: float}
 */
DEFINE Matrix__NormalizeCols(M)
RETURNS normalized {
    with_totals     =   FOREACH (GROUP $M BY col) GENERATE
                            FLATTEN($M) AS (row, col, val), (float) SUM($M.val) AS total;
    $normalized     =   FOREACH with_totals GENERATE
                            row, col, val / total AS val;
};

/*
 * For each row, take only the top $max_elems_per_row elements, ordered by $order_direction
 *
 * mat: {row: int, col: int, val: float}
 * order_direction: 'ASC' or 'DESC'
 * max_elems_per_row: int
 * -->
 * normalized: {row: int, col: int, val: float}
 */
DEFINE Matrix__TrimRows(mat, order_direction, max_elems_per_row)
returns trimmed {
    $trimmed        =   FOREACH (GROUP $mat BY row) {
                            ordered = ORDER $mat BY val $order_direction;
                            top     = LIMIT ordered $max_elems_per_row;
                            GENERATE FLATTEN(top) AS (row, col, val);
                        }
};

/*
 * Adds a "rank" field to each matrix element
 * containing its rank in the ordering of its row.
 *
 * mat: {row: int, col: int, val: float}
 * order_direction: 'ASC' or 'DESC'
 * max_elems_per_row: int
 * -->
 * ranked: {row: int, col: int, val: float, rank: int}
 */ 
DEFINE Matrix__RankRows(mat, order_direction)
RETURNS ranked {
    $ranked         =   FOREACH (GROUP $mat BY row) {
                            ordered = ORDER $mat BY val $order_direction;
                            GENERATE FLATTEN(Matrix__Enumerate(ordered))
                                     AS (row, col, val, rank);
                        }
};

----------------------------------------------------------------------------------------------------

/*
 * Other matrix utilities
 */

/*
 * Given a matrix where the row and column indices are chararrays instead of ints
 * (usually representing the adjacency matrix of a graph),
 * assigns integer ids to each distinct row and column id and substitutes these
 * as the indices of the matrix. This improves performance and allows the matrix
 * to use the other Matrix macros which require integer ids.
 *
 * mat: {row: chararray/bytearray, col: chararray/bytearray, val: float}
 * -->
 * mat_with_ids: {row: int, col: int, val: float}
 * id_map: {id: int, name: chararray/bytearray}
 */
DEFINE Matrix__NamesToIds(mat)
RETURNS mat_with_ids, id_map {
    row_names_dups  =   FOREACH $mat GENERATE row;
    col_names_dups  =   FOREACH $mat GENERATE col;
    row_names       =   DISTINCT row_ids_dups;
    col_names       =   DISTINCT col_ids_dups;
    all_names       =   UNION row_ids, col_ids;

    enum            =   FOREACH (GROUP all_names ALL) GENERATE
                            FLATTEN(Matrix__Enumerate(all_names))
                            AS (name, id);
    $id_map         =   FORAECH enum GENERATE id, name;

    join_1          =   FOREACH (JOIN $id_map BY name, $mat BY row) GENERATE
                            id AS row, col AS col, val AS val;
    $mat_with_ids   =   FOREACH (JOIN $id_map BY name, $mat BY col) GENERATE
                            row AS row, id AS col, val AS val;
};

/*
 * Inverse of Matrix__NamesToIds: given the int-indexed matrix
 * and the id map from that macro, returns the matrix to the chararray indicies.
 *
 * mat: {row: int, col: int, val: float}
 * id_map: {id: int, name: chararray/bytearray}
 * -->
 * mat_with_names: {row: chararray/bytearray, col: chararray/bytearray, val: float}
 */
DEFINE Matrix__IdsToNames(mat, id_map)
RETURNS mat_with_names {
    join_1          =   FOREACH (JOIN $id_map BY id, $mat BY col) GENERATE
                            row AS row, $1 AS col, val AS val;
    $mat_with_names =   FOREACH (JOIN $id_map BY id, join_1 BY row) GENERATE
                            $1 AS row, col AS col, val AS val;
};

/*
 * Visualizes a matrix row-wise:
 *     1) returns from integer ids to names
 *     2) groups elements by row and orders them
 *     3) takes only the top N elements per row
 * Useful for debugging algorithms.
 *
 * mat: {row: int, col: int, val: float}
 * id_map: {id: int, name: chararray}
 * ordering: 'ASC' or 'DESC'
 * max_terms_per_row: int
 * -->
 * visualization: {row: int, non_zero_terms: {(col: chararray/bytearray, val: float)}}
 */
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
