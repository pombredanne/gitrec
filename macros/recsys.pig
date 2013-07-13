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

IMPORT 'matrix.pig';
IMPORT 'graph.pig';
IMPORT 'normalization.pig';

----------------------------------------------------------------------------------------------------

DEFINE Recsys__UIScores_To_IILinks(ui_scores, min_link_weight)
RETURNS ii_links {
    ii_link_terms   =   Recsys__UIScores_To_IITerms($ui_scores);
    $ii_links       =   Recsys__IITerms_To_IILinks(ii_link_terms, $min_link_weight);
};

DEFINE Recsys__UIScores_To_IITerms(ui_scores)
RETURNS ii_link_terms {
    ui_copy         =   FOREACH $ui_scores GENERATE *;
    ui_joined       =   JOIN    $ui_scores BY user, ui_copy BY user;
    ui_filtered     =   FILTER   ui_joined BY $ui_scores::item != ui_copy::item;

    $ii_link_terms  =   FOREACH ui_filtered GENERATE
                            $ui_scores::item AS row,
                            ui_copy::item    AS col,
                            ($ui_scores::score < ui_copy::score ?
                                $ui_scores::score : ui_copy::score
                            ) AS val;
};

DEFINE Recsys__IITerms_To_IILinks(ii_link_terms, min_link_weight)
RETURNS ii_links {
    agg_ii_links    =   FOREACH (GROUP $ii_link_terms BY (row, col)) GENERATE
                            FLATTEN(group) AS (row, col),
                            (float) SUM($ii_link_terms.val) AS val;

    $ii_links       =   FILTER agg_ii_links BY val >= $min_link_weight;
};

DEFINE Recsys__IILinksRaw_To_IILinksBayes(ii_links_raw, bayesian_prior)
RETURNS ii_links_bayes {
    item_totals     =   FOREACH (GROUP $ii_links_raw BY row) GENERATE
                            group AS item,
                            SUM($ii_links_raw.val) AS total;

    $ii_links_bayes =   FOREACH (JOIN item_totals BY item, $ii_links_raw BY col) GENERATE
                            $ii_links_raw::row AS row,
                            $ii_links_raw::col AS col,
                            $ii_links_raw::val / (item_totals::total + $bayesian_prior) AS val;
};

----------------------------------------------------------------------------------------------------

DEFINE Recsys__IILinksShortestPathsTwoSteps(ii_links, neighborhood_size)
RETURNS ii_nhoods {
    distance_mat    =   FOREACH $ii_links  GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp      =   Graph__ShortestPathsTwoSteps(distance_mat, $neighborhood_size);
    nhoods_tmp_inv  =   FOREACH nhoods_tmp GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp_norm =   Normalization__LinearTransform(nhoods_tmp_inv, 'val', 'row, col');
    $ii_nhoods      =   FOREACH nhoods_tmp_norm GENERATE row, col, (float) val AS val;
};

DEFINE Recsys__IILinksShortestPathsThreeSteps(ii_links, neighborhood_size)
RETURNS ii_nhoods {
    distance_mat    =   FOREACH $ii_links  GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp      =   Graph__ShortestPathsThreeSteps(distance_mat, $neighborhood_size);
    nhoods_tmp_inv  =   FOREACH nhoods_tmp GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp_norm =   Normalization__LinearTransform(nhoods_tmp_inv, 'val', 'row, col');
    $ii_nhoods      =   FOREACH nhoods_tmp_norm GENERATE row, col, (float) val AS val;
};

----------------------------------------------------------------------------------------------------

DEFINE Recsys__IILinksPagerankTwoSteps(ii_links, teleport_prob, neighborhood_size)
RETURNS ii_nhoods {
    trans_mat       =   Matrix__NormalizeRows($ii_links);
    walk_step_1     =   Graph__RandomWalk_Init(trans_mat);
    walk_step_2     =   Graph__PersonalizedPagerank_Iterate(walk_step_1, trans_mat, $teleport_prob, $neighborhood_size);
    $ii_nhoods      =   Graph__RandomWalk_Complete(walk_step_2);
};

DEFINE Recsys__IILinksPagerankThreeSteps(ii_links, teleport_prob, neighborhood_size)
RETURNS ii_nhoods {
    trans_mat       =   Matrix__NormalizeRows($ii_links);
    walk_step_1     =   Graph__RandomWalk_Init(trans_mat);
    walk_step_2     =   Graph__PersonalizedPagerank_Iterate(walk_step_1, trans_mat, $teleport_prob, $neighborhood_size);
    walk_step_3     =   Graph__PersonalizedPagerank_Iterate(walk_step_2, trans_mat, $teleport_prob, $neighborhood_size);
    $ii_nhoods      =   Graph__RandomWalk_Complete(walk_step_3);
};

----------------------------------------------------------------------------------------------------

/*
 * ui_affinities: {user: int, item: int, affinity: float, reason_flag: int/chararray}
 * item_nhoods:   {item: int, neighbor: int, affinity: float}
 * -->
 * user_nhoods:   {user: int, item: int, affinity: float, reason: int, reason_flag: int/chararray} 
 */
DEFINE Recsys__UserItemNeighborhoods(ui_affinities, item_nhoods)
RETURNS user_nhoods {
    user_nhoods_tmp =   FOREACH (JOIN $ui_affinities BY item, $item_nhoods BY item) GENERATE
                                              user AS user,
                            $item_nhoods::neighbor AS item,
                            (float) SQRT($ui_affinities::affinity *
                                         $item_nhoods::affinity) AS affinity,
                                $item_nhoods::item AS reason,
                                       reason_flag AS reason_flag;

    -- hack to get around a NullPointerException bug in the TOP builtin UDF
    $user_nhoods    =   FOREACH (GROUP user_nhoods_tmp BY
                                 ((((long) (user + item) * (long) (user + item + 1)) / 2) + item)) {
                            sorted = ORDER user_nhoods_tmp BY affinity DESC;
                            best   = LIMIT sorted 1;
                            GENERATE FLATTEN(best) AS (user, item, affinity, reason, reason_flag);
                        }
};

/*
 * user_nhoods:   {user: int, item: int, affinity: float, reason: int, reason_flag: int/chararray}
 * ui_affinities: {user: int, item: int, affinity: float, reason_flag: int/chararray}
 * -->
 * user_nhoods_filt: {user: int, item: int, affinity: float, reason: int, reason_flag: int/chararray}
 */
DEFINE Recsys__FilterItemsAlreadySeenByUser(user_nhoods, ui_affinities)
RETURNS filtered {
    joined      =   JOIN $user_nhoods   BY (user, item) LEFT OUTER,
                         $ui_affinities BY (user, item);
    $filtered   =   FOREACH (FILTER joined BY $ui_affinities::item IS null) GENERATE
                               $user_nhoods::user AS user,
                               $user_nhoods::item AS item,
                           $user_nhoods::affinity AS affinity,
                             $user_nhoods::reason AS reason,
                        $user_nhoods::reason_flag AS reason_flag;
};

/*
 * user_nhoods: {user: int, item: int, affinity: float, reason: int, reason_flag: int/chararray}
 * num_recs:    int
 * --> 
 * user_recs: {user: int, item: int, affinity: float, reason: int, reason_flag: int/chararray}
 */
DEFINE Recsys__TopNUserRecs(user_nhoods, num_recs)
RETURNS user_recs {
    $user_recs  =   FOREACH (GROUP $user_nhoods BY user) {
                        sorted = ORDER $user_nhoods BY affinity DESC;
                        best   = LIMIT sorted $num_recs;
                        GENERATE FLATTEN(best) AS (user, item, affinity, reason, reason_flag);
                    }
};

/*
 * user_nhoods: {user: int, item: int, affinity: float, reason: int, reason_flag: int/chararray}
 * user_names:  {id: int, name: chararray}
 * item_names:  {id: int, name: chararray}
 * -->
 * with_names:  {user: chararray, item: chararray, affinity: float, reason: chararray, reason_flag: int/chararray}
 */
DEFINE Recsys__UserRecsIntegerIdsToNames(user_nhoods, user_names, item_names)
RETURNS with_names {
    join_1      =   FOREACH (JOIN $user_names BY id, $user_nhoods BY user) GENERATE
                        name AS user, item AS item, affinity AS affinity,
                        reason AS reason, reason_flag AS reason_flag;    
    join_2      =   FOREACH (JOIN $item_names BY id, join_1 BY item) GENERATE
                        user AS user, name AS item, affinity AS affinity,
                        reason AS reason, reason_flag AS reason_flag;
    $with_names =   FOREACH (JOIN $item_names BY id, join_2 BY reason) GENERATE
                        user AS user, item AS item, affinity AS affinity,
                        name AS reason, reason_flag AS reason_flag;
};
