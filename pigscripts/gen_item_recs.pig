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
 * These parameters default for running the gitrec on a subset of the data on your local machine
 * (see note in README.md about how to get the input data).
 * To run on a cluster, set the s3 locations you want in paramfiles/gitrec-cloud.params
 * and use "mortar:run pigscripts/gitrec.pig -f paramfiles/gitrec-cloud.params"
 */

%default USER_ITEM_SCORES_PATH '../../../data/github/user_item_scores'
%default ITEM_METADATA_PATH    '../../../data/github/item_metadata'
%default ITEM_IDS_PATH         '../../../data/github/item_ids'
%default ITEM_NHOODS_PATH      '../../../data/github/item_nhoods'
%default ITEM_RECS_PATH        '../../../data/github/item_recs'

%default DEFAULT_PARALLEL 1
SET default_parallel $DEFAULT_PARALLEL

/*
 * Algorithm parameters
 * There is no easy way to choose these; the values here were chosen using
 * just a combination of intuition and careful observation of intermediate outputs.
 */

%default MIN_LINK_WEIGHT    0.12     -- links between items will be filtered if their strength
                                     -- is less than this value by the metric we will calculate
%default BAYESIAN_PRIOR     25.0     -- this is to guard the collaborative filter
                                     -- against the effects of small sample sizes
%default MIN_REC_ITEM_SCORE 50.0     -- ensure that items below a minimum popularity threshold
                                     -- will never be recommended
%default NEIGHBORHOOD_SIZE  20       -- number of recommendations to output for each item

IMPORT 'matrix.pig';
IMPORT 'normalization.pig';
IMPORT 'recsys.pig';

----------------------------------------------------------------------------------------------------

ui_scores       =   LOAD '$USER_ITEM_SCORES_PATH' USING PigStorage()
                    AS (user: int, item: int,
                        specific_interest: float, general_interest: float, score: float,
                        mapped_from_fork: int);
ui_scores       =   FOREACH ui_scores GENERATE user, item, score;
ui_scores       =   FILTER ui_scores BY score > 0;

item_metadata   =   LOAD '$ITEM_METADATA_PATH' USING PigStorage()
                    AS (item: int,
                        activity: float, num_forks: int, num_stars: int, score: float,
                        language: chararray, description: chararray);
item_scores     =   FOREACH item_metadata GENERATE item, score;
item_scores     =   FILTER item_scores BY score >= $MIN_REC_ITEM_SCORE;

item_ids        =   LOAD '$ITEM_IDS_PATH' USING PigStorage()
                    AS (id: int, name: chararray);

----------------------------------------------------------------------------------------------------

/*
 * Reduce the graph of user-item affinities to a graph of item-item affinities.
 */

ii_links_raw    =   Recsys__UIScores_To_IILinks(ui_scores, $MIN_LINK_WEIGHT);

/*
 * Filter out links to items below the minimum threshold of popularity
 */

ii_links_filt   =   FOREACH (JOIN item_scores BY item, ii_links_raw BY col) GENERATE
                        row AS row, col AS col, val AS val;

/*
 * Use Bayes Theorem to estimate the probability of a user
 * interacting with item A given that they interacted with item B
 * and call that the affinity of A -> B.
 * These affinities represent "confidence" that items are similar and are asymmetric.
 */

ii_links_bayes  =   Recsys__IILinksRaw_To_IILinksBayes(ii_links_filt, $BAYESIAN_PRIOR);


/*
 * To improve performance, trim all but the top NEIGHBORHOOD_SIZE links for each item.
 */

ii_links        =   Matrix__TrimRows(ii_links_bayes, 'DESC', $NEIGHBORHOOD_SIZE);

/*
 * We define "distance" to be the reciprocal of "affinity",
 * so similar items are "close" and dissimilar items are "far".
 *
 * We then follow shortest paths on this distance graph between items.
 * This has two effects:
 *     1) if A -> B -> C is a shorter path than A -> D,
 *        we'll know to recommend C over D even though it is an indirect connection
 *     2) if an item only has a few links, following shortest paths
 *        will give it a full set of NEIGHBORHOOD_SIZE in most cases
 *
 * The macro we use re-inverts the weights on the graph again to go back to affinities from distances,
 * and renormalizes so that all values are between 0 and 1.
 */

item_nhoods     =   Recsys__IILinksShortestPathsTwoSteps(ii_links, $NEIGHBORHOOD_SIZE);

/*
 * Give a small boost to links to more popular items,
 * and renormalize all values to be between 0 and 1.
 */

item_nhoods     =   FOREACH (JOIN item_scores BY item, item_nhoods BY col) GENERATE
                        row AS row, col AS col,
                        val * (float) CBRT(score) AS val;
item_nhoods     =   Normalization__LinearTransform(item_nhoods, 'val', 'row, col');

----------------------------------------------------------------------------------------------------

/*
 * The item neighborhoods generated above are used to later in this script,
 * but we also output a version with names and metadata that serves as a
 * "repos similar to the given repo" recommender.
 */

item_nhoods_with_names  =   Matrix__IdsToNames(item_nhoods, item_ids);
item_nhoods_ranked      =   Matrix__RankRows(item_nhoods_with_names, 'DESC');

item_data_for_recs      =   FOREACH (JOIN item_ids BY id, item_metadata BY item) GENERATE
                                       name AS item,
                                   language AS language,
                                  num_forks AS num_forks,
                                  num_stars AS num_stars,
                                description AS description;

item_recs               =   FOREACH (JOIN item_nhoods_ranked BY col, item_data_for_recs BY item) GENERATE
                                        row AS item,
                                       rank AS rank,
                                        col AS rec,
                                   language AS language,
                                  num_forks AS num_forks,
                                  num_stars AS num_stars,
                                description AS description;

item_recs               =   FOREACH (GROUP item_recs BY item) {
                                sorted = ORDER item_recs BY rank ASC;
                                GENERATE FLATTEN(sorted);
                            }

----------------------------------------------------------------------------------------------------

rmf $ITEM_NHOODS_PATH;
rmf $ITEM_RECS_PATH;

STORE item_nhoods INTO '$ITEM_NHOODS_PATH' USING PigStorage();
STORE item_recs   INTO '$ITEM_RECS_PATH'   USING PigStorage();
