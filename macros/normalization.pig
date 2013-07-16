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
 * Map a field into the range [0, 1],
 * with the option of also applying a transform (SQRT or LOG) beforehand
 *
 * samples: any relation
 * field: chararray, name of the field to normalize
 * projection: comma-delimited list of the other fields in the relation
 * -->
 * normalized: {[schema of 'projection'], field}
 *
 * Example Usage:
 * tf_idfs: { id: int, term: chararray, tf_idf: double }
 * normalized = Stats__LogarithmicNormalization(tf_idfs, 'tf_idf', 'id, term');
 */

DEFINE Normalization__LinearTransform(samples, field, projection)
RETURNS normalized {
    stats           =   FOREACH (GROUP $samples ALL) GENERATE
                            MIN($samples.$field) AS min,
                            MAX($samples.$field) AS max;
    $normalized     =   FOREACH $samples GENERATE
                            $projection,
                            ($field - stats.min) / (stats.max - stats.min) AS $field; 
};

DEFINE Normalization__QuadraticTransform(samples, field, projection)
RETURNS normalized {
    sqrt_transform  =   FOREACH $samples GENERATE $projection, SQRT($field) AS $field;
    $normalized     =   Normalization__LinearTransform(sqrt_transform, '$field', '$projection');  
};

DEFINE Normalization__LogarithmicTransform(samples, field, projection)
RETURNS normalized {
    log_transform   =   FOREACH $samples GENERATE $projection, LOG($field) AS $field;
    $normalized     =   Normalization__LinearTransform(log_transform, '$field', '$projection');  
};
