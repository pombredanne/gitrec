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

-- To find shortest paths on a graph using Min-Plus Matrix Multiplication
-- (see matrix.pig), you need to first add 0-weight self loops to each vertex
-- (and then remove them when you are done with the pathfinding).

IMPORT 'matrix.pig';

----------------------------------------------------------------------------------------------------

-- All graphs represented as sparse adjacency matrices: { row: int, col: int, val: float }

DEFINE Graph__AddSelfLoops(graph)
RETURNS out_graph, vertices {
    from_vertices       =   FOREACH $graph GENERATE row AS id;
    to_vertices         =   FOREACH $graph GENERATE col AS id;
    vertices_with_dups  =   UNION from_vertices, to_vertices;
    $vertices           =   DISTINCT vertices_with_dups;
    self_loops          =   FOREACH $vertices GENERATE id AS row, id AS col, 0.0 AS val;
    $out_graph          =   UNION self_loops, $graph;
};

DEFINE Graph__ShortestPathsTwoSteps(graph, neighborhood_size)
RETURNS nhoods {
    graph, vertices     =   Graph__AddSelfLoops($graph);
    squared             =   Matrix__MinPlusSquared(graph);
    squared_trimmed     =   Matrix__TrimRows(squared, 'ASC', $neighborhood_size);
    $nhoods             =   FILTER squared_trimmed BY row != col;
};

DEFINE Graph__ShortestPathsThreeSteps(graph, neighborhood_size)
RETURNS nhoods {
    graph, vertices     =   Graph__AddSelfLoops($graph);
    squared             =   Matrix__MinPlusSquared(graph);
    squared_trimmed     =   Matrix__TrimRows(squared, 'ASC', $neighborhood_size);
    cubed               =   Matrix__MinPlusProduct(squared_trimmed, graph);
    cubed_trimmed       =   Matrix__TrimRows(cubed, 'ASC', $neighborhood_size);
    $nhoods             =   FILTER cubed_trimmed BY row != col;
};

----------------------------------------------------------------------------------------------------

DEFINE Graph__RandomWalk_Init(graph)
RETURNS walk_graph {
    $walk_graph         =   FOREACH $graph GENERATE *;
};

DEFINE Graph__RandomWalk_Step(prev_walk_step, trans_mat, neighborhood_size)
RETURNS new_walk_step {
    new_trans_mat       =   Matrix__Product($prev_walk_step, $trans_mat);
    $new_walk_step      =   Matrix__TrimRows(new_trans_mat, 'DESC', $neighborhood_size);
};

DEFINE Graph__RandomWalk_Complete(final_walk_step)
RETURNS walk_result {
    no_self_loops       =   FOREACH (FILTER $final_walk_step BY row != col) GENERATE row, col, val;
    $walk_result        =   Matrix__NormalizeRows(no_self_loops);
};

----------------------------------------------------------------------------------------------------

DEFINE Graph__RandomWalk_TeleportToSource(prev_walk_step, teleport_prob)
RETURNS new_walk_step {
    damped              =   FOREACH $prev_walk_step GENERATE
                                row, col,
                                (1.0 - $teleport_prob) * val AS val;
    sources             =   FOREACH $prev_walk_step GENERATE row;
    teleports           =   FOREACH (DISTINCT sources) GENERATE
                                row, row AS col, $teleport_prob AS val;
    $new_walk_step      =   UNION damped, teleports;
};

DEFINE Graph__PersonalizedPagerank_Iterate(prev_walk_step, trans_mat, teleport_prob, neighborhood_size)
RETURNS think_of_home {
    go_on_an_adventure  =     Graph__RandomWalk_Step($prev_walk_step, $trans_mat, $neighborhood_size);
    $think_of_home      =     Graph__RandomWalk_TeleportToSource(go_on_an_adventure, $teleport_prob);
};
