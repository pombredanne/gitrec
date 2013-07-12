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

-- SET default_parallel $DEFAULT_PARALLEL;

/*
 * These parameters default for running the gitrec on a subset of the data on your local machine
 * (see note in README.md about how to get the input data).
 * to run on a cluster, set the s3 locations you want in paramfiles/gitrec-cloud.params
 * and use "mortar:run pigscripts/gitrec.pig -f paramfiles/gitrec-cloud.params"
 */

%default EVENT_LOGS_INPUT_PATH         '../../../data/github/raw_events/2013/06/{01,02,03}/*'
%default USER_IDS_OUTPUT_PATH          '../../../data/github/user_ids'
%default ITEM_IDS_OUTPUT_PATH          '../../../data/github/item_ids'
%default USER_GRAVATAR_IDS_OUTPUT_PATH '../../../data/github/user_gravatar_ids'
%default USER_ITEM_SCORES_OUTPUT_PATH  '../../../data/github/user_item_scores'
%default ITEM_METADATA_OUTPUT_PATH     '../../../data/github/item_metadata'
%default ITEM_RECS_OUTPUT_PATH         '../../../data/github/item_recs'

/*
 * Algorithm parameters
 * There is no easy way to choose these; the values here were chosen using
 * just a combination of intuition and careful observation of intermediate outputs.
 */

%default MIN_LINK_WEIGHT    0.12     -- links between items will be filtered if their strength
                                     -- is less than this value by the metric we will calculate
%default BAYESIAN_PRIOR     25.0     -- this is to guard the collaborative filter
                                     -- against the effects of small sample sizes
%default MIN_REC_ITEM_SCORE 3.0      -- this ensures that items below a minimum popularity threshold
                                     -- will never be recommended
%default NEIGHBORHOOD_SIZE  20       -- generate this many recommendations per item

REGISTER 'gitrec_udfs.py' USING jython AS udfs;
IMPORT   'matrix.pig';
IMPORT   'normalization.pig';
IMPORT   'recsys.pig';
IMPORT   'utils.pig';

----------------------------------------------------------------------------------------------------

-- load the raw event logs
events          =   LOAD '$EVENT_LOGS_INPUT_PATH'
                    USING org.apache.pig.piggybank.storage.JsonLoader('
                        actor: chararray,
                        actor_attributes: (
                            gravatar_id: chararray,
                            company: chararray
                        ),
                        repository: (
                            owner: chararray,
                            name: chararray,
                            fork: chararray,
                            language: chararray,
                            description: chararray,
                            watchers: int,
                            stargazers: int,
                            forks: int
                        ),
                        type: chararray,
                        created_at: chararray
                    ');

-- filter to take only events that signal that a user
-- is interested in or a contributor to a repo: Fork, Pull Request, Push, and Watch
events          =   FILTER events BY (
                        (
                            actor            IS NOT NULL AND
                            repository.owner IS NOT NULL AND 
                            repository.name  IS NOT NULL AND
                            repository.fork  IS NOT NULL AND
                            type             IS NOT NULL AND
                            created_at       IS NOT NULL
                        )

                        AND

                        (
                            type == 'ForkEvent'        OR
                            type == 'PullRequestEvent' OR
                            type == 'PushEvent'        OR
                            type == 'WatchEvent'
                        )
                    );

events_renamed  =   FOREACH events GENERATE
                        actor AS user,
                        CONCAT(repository.owner, CONCAT('/', repository.name)) AS item: chararray,
                        repository AS metadata,
                        type,
                        created_at AS timestamp;

-- some logs seem to be malformed and have empty strings
-- for the repository.owner field, so we filter those out
events_renamed  =   FILTER events_renamed BY SUBSTRING(item, 0, 1) != '/';

----------------------------------------------------------------------------------------------------

-- assign an integer id for each user and repo name for better performance
-- we will go back from ids to names when we output final recommendations

user_ids            =   Utils__AssignIntegerIds(events_renamed, 'user');
item_ids            =   Utils__AssignIntegerIds(events_renamed, 'item');

parsed_events       =   FOREACH (JOIN user_ids BY name, events_renamed BY user) GENERATE
                                   id AS user,
                                 item AS item,
                             metadata AS metadata,
                                 type AS type,
                            timestamp AS timestamp;
parsed_events       =   FOREACH (JOIN item_ids BY name, parsed_events BY item) GENERATE
                                 user AS user,
                                   id AS item,
                             metadata AS metadata,
                                 type AS type,
                            timestamp AS timestamp;

-- our model does not use content-based filtering or temporal information, so we throw those out
-- (we show repo metadata from parsed_events for each recommendation at the end however)

parsed_events_trim  =   FOREACH parsed_events GENERATE user, item, type;

----------------------------------------------------------------------------------------------------

-- get the gravatar ids for each user
-- you can get an image from a gravatar id by hitting:
-- http://www.gravatar.com/avatar/[the gravatar id]

events_for_gravatar =   FOREACH (FILTER events BY SIZE(actor_attributes.gravatar_id) == 32) GENERATE
                            actor AS user,
                            actor_attributes.gravatar_id AS gravatar_id,
                            created_at AS timestamp;

latest_by_user      =   FOREACH (GROUP events_for_gravatar BY user) GENERATE
                            FLATTEN(TOP(1, 2, events_for_gravatar))
                            AS (user, gravatar_id, timestamp);

gravatar_ids        =   FOREACH latest_by_user GENERATE user, gravatar_id;

----------------------------------------------------------------------------------------------------

-- give a weighting to each event and aggregate for each unique (user, item) pair
-- to get an "affinity score". Then we apply a logistic scaling function to map every affinity score
-- to a value between 0 and 1, so if a user pushes many many times to a repo,
-- they won't get a unreasonably high affinity score that would mess up later steps in the algorithm.
-- see udfs/jython/gitrec_udfs.py

events_valued   =   FOREACH parsed_events_trim GENERATE
                        user, item, 
                        FLATTEN(udfs.value_event(type))
                        AS (specific_interest, general_interest, graph_score);

ui_totals       =   FOREACH (GROUP events_valued BY (user, item)) GENERATE
                        FLATTEN(group) AS (user, item),
                        (float) SUM(events_valued.specific_interest) AS specific_interest,
                        (float) SUM(events_valued.general_interest)  AS general_interest,
                        (float) SUM(events_valued.graph_score)       AS graph_score;

ui_scores       =   FOREACH ui_totals GENERATE
                        user, item,
                        FLATTEN(udfs.scale_ui_scores(specific_interest, general_interest, graph_score))
                        AS (specific_interest, general_interest, graph_score);

ui_scores       =   FOREACH ui_scores GENERATE
                        user, item,
                        (float) specific_interest,
                        (float) general_interest,
                        (float) graph_score AS score;

-- aggregate affinity scores for each unique repo

item_activities =   FOREACH (GROUP ui_scores BY item) GENERATE
                        group AS item,
                        (float) SUM(ui_scores.score) AS activity;

----------------------------------------------------------------------------------------------------

-- if a user interact with a fork of a repo, in almost all cases
-- it is better to give recommendations based on the original repo for that fork
-- instead of the fork itself.

events_for_fork_map =   FOREACH parsed_events GENERATE
                            item AS item,
                            metadata.name AS repo_name,
                            (metadata.fork == 'true'? 0 : 1) AS is_not_a_fork;

unique_items        =   DISTINCT events_for_fork_map;
original_items      =   FOREACH (GROUP unique_items BY repo_name) GENERATE
                            FLATTEN(TOP(1, 2, unique_items))
                            AS (original_item, repo_name, is_not_a_fork);

fork_map            =   FOREACH (JOIN original_items BY repo_name, unique_items BY repo_name) GENERATE
                            item AS item, original_item AS original_item;

ui_scores           =   FOREACH (JOIN fork_map BY item, ui_scores BY item) GENERATE
                                         user AS user,
                                original_item AS item,
                            specific_interest AS specific_interest,
                             general_interest AS general_interest,
                                        score AS score,
                            (fork_map::item != fork_map::original_item ? 1 : 0) AS mapped_from_fork;

----------------------------------------------------------------------------------------------------

-- we have repo metadata with every event, but we only want the metadata
-- for the most recent state of the repo

most_recent_events  =   FOREACH (GROUP parsed_events BY item) GENERATE
                            FLATTEN(TOP(1, 4, parsed_events))
                            AS (user, item, metadata, type, created_at);

item_metadata       =   FOREACH most_recent_events GENERATE
                            item,
                            (metadata.fork == 'true'? 0 : 1) AS is_valid_rec: int,
                            (metadata.language is null ? 'Unknown' : metadata.language) AS language,
                            metadata.forks AS num_forks,
                            metadata.stargazers AS num_stars,
                            metadata.description AS description,
                            2 * metadata.forks + metadata.stargazers AS popularity;

-- the "score" field is a combined measure of popularity and activity
-- this is necessary because repos like django-old have lots of stars,
-- but are abandoned, so they should not be considered as recommendations

item_metadata       =   FOREACH (JOIN item_activities BY item, item_metadata BY item) GENERATE
                                            item_metadata::item AS item,
                                                   is_valid_rec AS is_valid_rec,
                                                       activity AS activity,
                                                      num_forks AS num_forks,
                                                      num_stars AS num_stars,
                            (float) SQRT(popularity * activity) AS score,
                                                       language AS language,
                                                    description AS description;

----------------------------------------------------------------------------------------------------

/*
 * Reduce the graph of user-item affinities to a graph of item-item affinities.
 * and filter out links to forks of repos, as they are unlikely to be of general interest.
 */

ii_links_raw    =   Recsys__UIScores_To_IILinks(ui_scores, $MIN_LINK_WEIGHT);
metadata_tmp    =   FOREACH item_metadata GENERATE item, is_valid_rec;
joined          =   JOIN ii_links_raw BY col, metadata_tmp BY item;
filtered        =   FILTER joined BY is_valid_rec == 1;
ii_links_raw    =   FOREACH filtered GENERATE row AS row, col AS col, val AS val;

/*
 * Use Bayes Theorem to estimate the probability of a user
 * interacting with item A given that they interacted with item B
 * and call that the affinity of A -> B.
 * These affinities represent "confidence" that items are similar and are asymmetric.
 */

ii_links_bayes  =   Recsys__IILinksRaw_To_IILinksBayes(ii_links_raw, $BAYESIAN_PRIOR);

/*
 * Give a small boost to links to more popular items,
 * filter out items below a minimum popularity threshold,
 * and renormalize all values to be between 0 and 1.
 */

item_scores     =   FOREACH (FILTER item_metadata BY score >= $MIN_REC_ITEM_SCORE) GENERATE item, score; 
ii_links_boost  =   FOREACH (JOIN item_scores BY item, ii_links_bayes BY col) GENERATE
                        row AS row, col AS col,
                        val * (float) LOG(score) AS val;
ii_links_boost  =   Normalization__LinearTransform(ii_links_boost, 'val', 'row, col');

 /*
  * To improve performance, trim all but the top NEIGHBORHOOD_SIZE links for each item.
  */

ii_links        =   Matrix__TrimRows(ii_links_boost, 'DESC', $NEIGHBORHOOD_SIZE);

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

item_nhoods     =   Recsys__IILinksShortestPathsThreeSteps(ii_links, $NEIGHBORHOOD_SIZE);

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

rmf $USER_IDS_OUTPUT_PATH;
rmf $ITEM_IDS_OUTPUT_PATH;
rmf $USER_GRAVATAR_IDS_OUTPUT_PATH;
rmf $USER_ITEM_SCORES_OUTPUT_PATH;
rmf $ITEM_METADATA_OUTPUT_PATH;
rmf $ITEM_RECS_OUTPUT_PATH;

STORE user_ids      INTO '$USER_IDS_OUTPUT_PATH'          USING PigStorage();
STORE item_ids      INTO '$ITEM_IDS_OUTPUT_PATH'          USING PigStorage();
STORE gravatar_ids  INTO '$USER_GRAVATAR_IDS_OUTPUT_PATH' USING PigStorage();
STORE ui_scores     INTO '$USER_ITEM_SCORES_OUTPUT_PATH'  USING PigStorage();
STORE item_metadata INTO '$ITEM_METADATA_OUTPUT_PATH'     USING PigStorage();
STORE item_recs     INTO '$ITEM_RECS_OUTPUT_PATH'         USING PigStorage();
