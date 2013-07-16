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

%default USER_ITEM_SCORES_PATH  '../../../data/github/user_item_scores'
%default ITEM_NHOODS_PATH       '../../../data/github/item_nhoods'
%default ITEM_METADATA_PATH     '../../../data/github/item_metadata'
%default USER_IDS_PATH          '../../../data/github/user_ids'
%default ITEM_IDS_PATH          '../../../data/github/item_ids'
%default USER_CONTRIB_RECS_PATH '../../../data/github/user_contrib_recs'
%default USER_GENERAL_RECS_PATH '../../../data/github/user_general_recs'

%default DEFAULT_PARALLEL 1
SET default_parallel $DEFAULT_PARALLEL

%default MIN_REC_ITEM_SCORE 3.0
%default NUM_CONTRIB_RECS 20
%default NUM_GENERAL_RECS 20

REGISTER 'datafu-0.0.10.jar';
DEFINE Enumerate datafu.pig.bags.Enumerate('1');

IMPORT   'matrix.pig';
IMPORT   'recsys.pig';

----------------------------------------------------------------------------------------------------

ui_scores       =   LOAD '$USER_ITEM_SCORES_PATH' USING PigStorage()
                    AS (user: int, item: int,
                        specific_interest: float, general_interest: float, score: float,
                        mapped_from_fork: int);

item_nhoods     =   LOAD '$ITEM_NHOODS_PATH' USING PigStorage()
                    AS (item: int, neighbor: int, affinity: float);

item_metadata   =   LOAD '$ITEM_METADATA_PATH' USING PigStorage()
                    AS (item: int,
                        activity: float, num_forks: int, num_stars: int, score: float,
                        language: chararray, description: chararray);
item_scores     =   FOREACH item_metadata GENERATE item, score;
item_scores     =   FILTER item_scores BY score >= $MIN_REC_ITEM_SCORE;

user_names      =   LOAD '$USER_IDS_PATH' USING PigStorage()
                    AS (id: int, name: chararray);
item_names      =   LOAD '$ITEM_IDS_PATH' USING PigStorage()
                    AS (id: int, name: chararray);

----------------------------------------------------------------------------------------------------

/*
 * Given an affinity score between user U and item I,
 * calulate affinities between U and each item in the neighborhood of I.
 *
 * We calculate two types of affinities:
 *     1) "contrib", which measures how similar a repo is to those you've contributed to,
 *     2) "general", which measures how similar a repo is to those you've watched/forked
 *
 * In the case that a user is linked to a neighbor by two original items,
 * we only consider the path with the highest overall affinity (no aggregation).
 */

ui_contrib_affinities   =   FOREACH ui_scores GENERATE
                            user, item, specific_interest AS affinity, mapped_from_fork AS reason_flag;
ui_contrib_affinities   =   FILTER ui_contrib_affinities BY affinity > 0;

user_nhoods_contrib     =   Recsys__UserItemNeighborhoods(ui_contrib_affinities, item_nhoods);

ui_general_affinities   =   FOREACH ui_scores GENERATE
                            user, item, general_interest AS affinity, mapped_from_fork AS reason_flag;
ui_general_affinities   =   FILTER ui_general_affinities BY affinity > 0;

user_nhoods_general     =   Recsys__UserItemNeighborhoods(ui_general_affinities, item_nhoods);

----------------------------------------------------------------------------------------------------

user_nhoods_contrib     =   Recsys__FilterItemsAlreadySeenByUser(user_nhoods_contrib, ui_scores);
contrib_recs_tmp        =   Recsys__TopNUserRecs(user_nhoods_contrib, $NUM_CONTRIB_RECS);

-- the extra filter is so that we don't show recommendations in general that are already in contrib
user_nhoods_general     =   Recsys__FilterItemsAlreadySeenByUser(user_nhoods_general, ui_scores);
user_nhoods_general     =   Recsys__FilterItemsAlreadySeenByUser(user_nhoods_general, contrib_recs_tmp);
general_recs_tmp        =   Recsys__TopNUserRecs(user_nhoods_general, $NUM_GENERAL_RECS);

----------------------------------------------------------------------------------------------------

/*
 * Postprocess the recommendations:
 *    Return from integer ids to names
 *    Filter out repos the user owns
 *    Rank recommendations
 *    Join with item metadata relation
 */

DEFINE PostprocessRecs(recs, user_names, item_names, item_metadata)
RETURNS postprocessed {
    recs_with_names =   Recsys__UserRecsIntegerIdsToNames($recs, $user_names, $item_names);
    filtered        =   FILTER recs_with_names BY user != REGEX_EXTRACT(item, '(.*)/(.*)', 1);
    ranked          =   FOREACH (GROUP filtered BY user) {
                            sorted = ORDER filtered BY affinity DESC;
                            GENERATE FLATTEN(Enumerate(sorted))
                                     AS (user, item, affinity, reason, reason_flag, rank);
                        }
    joined_1        =   JOIN $item_names BY name, ranked BY item;
    joined_2        =   FOREACH (JOIN $item_metadata BY item, joined_1 BY id) GENERATE
                                      user AS user,
                                      rank AS rank,
                              ranked::item AS item,
                                    reason AS reason,
                               reason_flag AS reason_flag,
                                 num_forks AS num_forks,
                                 num_stars AS num_stars,
                                  language AS language,
                               description AS description;
    $postprocessed  =   FOREACH (GROUP joined_2 BY user) {
                            sorted = ORDER joined_2 BY rank ASC;
                            GENERATE FLATTEN(sorted);
                        }
}

contrib_recs    =   PostprocessRecs(contrib_recs_tmp, user_names, item_names, item_metadata);
general_recs    =   PostprocessRecs(general_recs_tmp, user_names, item_names, item_metadata);

----------------------------------------------------------------------------------------------------

rmf $USER_CONTRIB_RECS_PATH;
rmf $USER_GENERAL_RECS_PATH;

STORE contrib_recs INTO '$USER_CONTRIB_RECS_PATH' USING PigStorage();
STORE general_recs INTO '$USER_GENERAL_RECS_PATH' USING PigStorage();
