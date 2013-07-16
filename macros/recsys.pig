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

IMPORT 'matrix.pig';
IMPORT 'graph.pig';
IMPORT 'normalization.pig';

%default RECSYS_KNN_K 10
DEFINE Recsys__FromPigCollectionToBag         com.mortardata.pig.collections.FromPigCollectionToBag();
DEFINE Recsys__KNearestNeighbors              com.mortardata.pig.geometry.KNearestNeighbors('$RECSYS_KNN_K');
DEFINE Recsys__LoadBalancingReducerAllocation com.mortardata.pig.partitioners.LoadBalancingReducerAllocation();
DEFINE Recsys__MergeAndKeepTopN               com.mortardata.pig.collections.MergeAndKeepTopN('$RECSYS_KNN_K');
DEFINE Recsys__ReplicateByKey                 com.mortardata.pig.collections.ReplicateByKey();

----------------------------------------------------------------------------------------------------
/*
 * This file contains macros for building recommender systems.
 *
 * The basic pipline for a collaborative filter goes:
 *    1) Recsys__UIScores_To_IILinks
 *    2) Recsys__IILinksRaw_To_IILinksBayes
 *    3) Recsys__IILinksShortestPathsTwoSteps (or Recsys__IILinksShortestPathsThreeSteps or other alternatives)
 *    -- by this point you get item-to-item recommendations
 *    4) Recsys__UserItemNeighborhoods
 *    5) Recsys__FilterItemsAlreadySeenByUser
 *    6) Recsys__TopNUserRecs
 *    -- by this point you get user-to-item recommendations
 *
 * See the comments for each macro for more details.
 */
----------------------------------------------------------------------------------------------------

/*
 * Step 1 of the default collaborative filter.
 *
 * Given a relation with weighted mappings of users to items
 * (representing how much evidence there is that the user likes that item,
 *  assumed to be 0 if a user-item pair is not present in the relation),
 * contstructs a similarity matrix between items.
 * 
 * Algorithmically, this is "contracting" the bipartite user-item graph
 * into a regular graph of item similarities. If a user U has affinity
 * with items I1 and I2 of weights W1 and W2 respectively, than a link
 * between I1 and I2 is formed with the weight MIN(W1, W2).
 *
 * ui_scores: {user: int, item: int, score: float}
 * min_link_weight: int/float, item-item links of less weight than this
 *                             will be filtered out for better performance
 * -->
 * ii_links: {row: int, col: int, float: val}
 */
DEFINE Recsys__UIScores_To_IILinks(ui_scores, min_link_weight)
RETURNS ii_links {
    ii_link_terms   =   Recsys__UIScores_To_IITerms($ui_scores);
    $ii_links       =   Recsys__IITerms_To_IILinks(ii_link_terms, $min_link_weight);
};

/*
 * Helper macro for Recsys__UIScores_To_IILinks
 *
 * ui_scores: {user: int, item: int, score: float}
 * -->
 * ii_link_terms: {row: int, col: int, val: float}
 */
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

/*
 * Helper macro for Recsys__UIScores_To_IILinks
 *
 * ii_link_terms: {row: int, col: int, val: float}
 * min_link_weight: int/float
 * -->
 * ii_links: {row: int, col: int, val: float}
 */
DEFINE Recsys__IITerms_To_IILinks(ii_link_terms, min_link_weight)
RETURNS ii_links {
    agg_ii_links    =   FOREACH (GROUP $ii_link_terms BY (row, col)) GENERATE
                            FLATTEN(group) AS (row, col),
                            (float) SUM($ii_link_terms.val) AS val;

    $ii_links       =   FILTER agg_ii_links BY val >= $min_link_weight;
};

/*
 * Step 2 of the default collaborative filter.
 *
 * The item-to-item links outputted by Step 1 (Recsys__UIScores_To_IILinks)
 * do not account for the popularity of the item linked to. Without accounting
 * for this, popular items will be considered "most-similar" for every other item,
 * since users of the other items frequently interact wit the popular item.
 *
 * This macro uses Bayes theorem to avoid this problem, and also scaled the
 * item-to-item links to all be within the range [0, 1]. It sets the similarity
 * of repos A and B to be an estimate of the probability that a random user U
 * will interact with A given that they interacted with B. In mathematical notation,
 * it similarity(A, B) = P(A | B). This way, if B is very popular, you need a lot
 * of users co-interacting with it and A for the link to be statistically significant.
 *
 * This estimation breaks down if B is very unpopular, with only a few users interacting with it.
 * If B only has 2 users and they all interacted with A, that is most likely due to chance, not similarity.
 * The macro therefore takes a Bayesian Prior, which guards against these small sample sizes.
 * Intuitively, it represents a number of "pseudo-observations" of the non-similarity of A and B;
 * or in other words, A is "innocent of B until proven guilty beyond a reasonable doubt".
 *
 * ii_links_raw: {row: int, col: int, val: float}
 * bayesian_prior: int/float
 * -->
 * ii_links_bayes: {row: int, col: int, val: float}
 */
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

/*
 * Step 3 of the default collaborative filter.
 *
 * The graph generated by Step 2 (Recsys__IILinksRaw_To_IILinksBayes)
 * is a good guess of similarity between items, but it can only contain
 * edges between items that were directly interacted with by the same user.
 * For obscure items with only a handful of users interacting with it,
 * it might not have as many links as we would like to make recommendations
 *
 * To get around this, we follow shortest paths on the graph, defining
 * distance to be the inverse of the similarity scores. This macro explores
 * the 2-neighborhood of each source node; the macro below this explores
 * the 3-neighborhood.
 *
 * This also has the effect that if there is a path from items
 * A -> B -> C that has a total distance of less than A -> D,
 * then the former path is recognized is more relevant; that is,
 * the link A -> C will be ranked higher than A -> D.
 *
 * For performance reasons, only the neighborhood_size most promising
 * paths at each iteration step are considered.
 *
 * ii_links: {row: int, col: int, val: float}
 * neighborhood_size: int
 * -->
 * ii_nhoods: {row: int, col: int, val: float}
 */
DEFINE Recsys__IILinksShortestPathsTwoSteps(ii_links, neighborhood_size)
RETURNS ii_nhoods {
    distance_mat    =   FOREACH $ii_links  GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp      =   Graph__ShortestPaths_TwoSteps(distance_mat, $neighborhood_size);
    nhoods_tmp_inv  =   FOREACH nhoods_tmp GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp_norm =   Normalization__LinearTransform(nhoods_tmp_inv, 'val', 'row, col');
    $ii_nhoods      =   FOREACH nhoods_tmp_norm GENERATE row, col, (float) val AS val;
};

/*
 * See notes for Recsys__IILinksShortestPathsTwoSteps.
 *
 * ii_links: {row: int, col: int, val: float}
 * neighborhood_size: int
 * -->
 * ii_nhoods: {row: int, col: int, val: float}
 */
DEFINE Recsys__IILinksShortestPathsThreeSteps(ii_links, neighborhood_size)
RETURNS ii_nhoods {
    distance_mat    =   FOREACH $ii_links  GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp      =   Graph__ShortestPaths_ThreeSteps(distance_mat, $neighborhood_size);
    nhoods_tmp_inv  =   FOREACH nhoods_tmp GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp_norm =   Normalization__LinearTransform(nhoods_tmp_inv, 'val', 'row, col');
    $ii_nhoods      =   FOREACH nhoods_tmp_norm GENERATE row, col, (float) val AS val;
};

----------------------------------------------------------------------------------------------------

/*
 * Step 4 for the default collaborative filter.
 *
 * Steps 1-3 gave us item-to-item recommendations.
 * Steps 4-6 will use those item-to-item recommendations to generate personalized recommendations.
 * This macro takes links between users and items, and the item-to-item recommendations,
 * and generates "user neighborhoods" consisting of all the items recommended for any item
 * the user has a link to.
 *
 * The "reason" for a recommendation is the item that a user was linked to that caused
 * the neighboring item to be recommended. "reason_flag" can contain additional
 * metadata about why the item was recommended: for example, in the Github Recommender,
 * if a user was linked to a fork of a repo, we wanted to give recommendations based
 * on the original repo that that forked, since that would have more data.
 * The reason_flag field was used to note when we were doing this mapping,
 * so the user could be informed of this on the front-end.
 *
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
 * Step 5 in the default collaborative filter.
 *
 * This macro filters the output of Step 4 (Recsys__UserItemNeighborhoods)
 * so that a user is never shown an item they are known to have already seen.
 * You may also wish to apply your own domain-specific filters to the user neighborhoods.
 *
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
 * Step 6 of the default collaborative filter.
 *
 * Takes the top N recommendations from a user's neighborhood after any filtering has been applied.
 *
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
 * This is a utility to return the output of the default collaborative filter
 * from integer ids to names for debugging.
 *
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
