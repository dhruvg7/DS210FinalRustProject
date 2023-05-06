// Import necessary libraries
use petgraph::graph::NodeIndex; // Import NodeIndex struct from petgraph library
use petgraph::{Graph as PetGraph, Directed}; // Import Graph struct from petgraph library with Directed edge direction
use petgraph::visit::{Bfs}; // Import Bfs
use std::error::Error; // Import Error trait
use csv::ReaderBuilder; // Import ReaderBuilder struct from csv library for reading CSV files
use rand::prelude::SliceRandom; // Import SliceRandom trait from rand to shuffle vectors

// Read in movie.csv
fn load_movies(filename: &str) -> Result<Vec<(u32, String)>, Box<dyn Error>> {
    let mut movies = Vec::new();
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b',')
        .from_path(filename)?;

    // Load only the first 2000 movies to save memory
    for (i, result) in reader.records().enumerate() {
        if i == 2000 {
            break;
        }
        let record = match result {
            Ok(record) => record,
            Err(e) => {
                eprintln!("Error reading record: {:?}", e);
                continue;
            }
        };
        let id = match record[0].parse::<u32>() {
            Ok(id) => id,
            Err(e) => {
                eprintln!("Error parsing id: {:?}", e);
                continue;
            }
        };
        let title = record[1].to_owned();
        movies.push((id, title));
    }

    Ok(movies)
}
// Read in rating.csv
fn load_ratings(filename: &str) -> Result<Vec<(u32, u32, f32)>, Box<dyn Error>> {
    let mut ratings = Vec::new();
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b',')
        .from_path(filename)?;

    for result in reader.records() {
        let record = match result {
            Ok(record) => record,
            Err(e) => {
                eprintln!("Error reading record: {:?}", e);
                continue;
            }
        };
        let movie_id = match record[1].parse::<u32>() {
            Ok(movie_id) => movie_id,
            Err(e) => {
                eprintln!("Error parsing movie_id: {:?}", e);
                continue;
            }
        };
        let user_id = match record[0].parse::<u32>() {
            Ok(user_id) => user_id,
            Err(e) => {
                eprintln!("Error parsing user_id: {:?}", e);
                continue;
            }
        };        
        let rating = match record[2].parse::<f32>() {
            Ok(rating) => rating,
            Err(e) => {
                eprintln!("Error parsing rating: {:?}", e);
                continue;
            }
        };
        if movie_id <= 100 {
            ratings.push((user_id, movie_id, rating));     
        }
    }
    Ok(ratings)
}

// Function that returns a graph with nodes representing movies and edges representing ratings assigned by users to movies.
fn create_movie_graph(movies: &[(u32, String)], ratings: &[(u32, u32, f32)]) -> PetGraph<String, f32, Directed> {
    // Create an empty PetGraph with node type String and edge type f32, and with directed edges
    let mut graph = PetGraph::<String, f32, Directed>::new();
    
    // Create a vector to hold the node indices for the movies in the graph
    let mut movie_nodes = Vec::with_capacity(movies.len());
    
    // Add nodes for each movie in the movies slice, starting from the 10th element and taking movies.len() - 20 elements.
    for &(_movie_id, ref title) in movies.iter().skip(10).take(movies.len() - 20) {
    // Add a node to the graph with the title of the movie as the node label and store its index in node_index
    let node_index = graph.add_node(title.clone());
    // Store the node index in the `movie_nodes` vector
    movie_nodes.push(node_index);
    }

    // Add edges for each rating in the ratings slice, starting from the 10th element and taking ratings.len() - 20 elements.
    for &(user_id, movie_id, rating) in ratings.iter().skip(10).take(ratings.len() - 20) {
    // Subtract 1 from movie_id to get the index of the corresponding node in movie_nodes
    let movie_node_index = (movie_id - 1) as usize;
    // If `movie_node_index` is out of bounds of `movie_nodes`, skip this iteration of the loop
    if movie_node_index >= movie_nodes.len() {
        continue;   
    }

    // Get the source and target node indices for the edge
    let source = movie_nodes[movie_node_index];
    let target = NodeIndex::new((user_id - 1) as usize);

    // If the source and target node indices are within the bounds of the graph, add the edge with the `rating` as the weight
    if source.index() < graph.node_count() && target.index() < graph.node_count() {
        // If the graph does not already contain an edge between `source` and `target`, add the edge with `rating` as the weight
        if !graph.contains_edge(source, target) {
            graph.add_edge(source, target, rating);
            }
        }
    }

    graph
}

fn bfs(graph: &petgraph::Graph<String, f32, petgraph::Directed>, start: NodeIndex, end: NodeIndex) -> Option<Vec<NodeIndex>> {
    // Create a new BFS iterator starting from the given start node.
    let mut bfs = Bfs::new(&graph, start);

    // Create a vector to keep track of the parents of each node.
    let mut parents = vec![None; graph.node_count()];

    // Loop through all nodes in the graph reachable from the start node using BFS.
    while let Some(node) = bfs.next(&graph) {
        // If we have reached the end node, backtrack from it to the start node to find the path.
        if node == end {
            // Create a vector to store the path from end to start.
            let mut path = Vec::new();
            let mut current = end;

            // Backtrack from end to start by following the parent links.
            while let Some(parent) = parents[current.index()] {
                path.push(current);
                current = parent;
            }

            // Add the start node to the path and reverse it so that it goes from start to end.
            path.push(start);
            path.reverse();

            // Return the path as Some(path).
            return Some(path);
        }

        // Otherwise, update the parents of all neighbors of the current node that have not been visited yet.
        for neighbor in graph.neighbors(node) {
            if parents[neighbor.index()].is_none() {
                parents[neighbor.index()] = Some(node);
            }
        }
    }

    // If we have not found a path from start to end, return None.
    None
}

fn main() -> Result<(), Box<dyn Error>> {
    // Load data from dataset
    let movies = load_movies("/Users/dhruvgandhi/Desktop/DS210/FinalRustProject/src/movie.csv")?;
    let ratings = load_ratings("/Users/dhruvgandhi/Desktop/DS210/FinalRustProject/src/rating.csv")?;

    // Print the number of movies and ratings
    println!("Number of movies: {}", movies.len());
    println!("Number of ratings: {}", ratings.len());

    // Create graph from the data
    let graph = create_movie_graph(&movies, &ratings);

    // Get total number of nodes and edges in the graph
    let total_nodes = graph.node_count();   
    println!("Total number of nodes: {}", total_nodes);
    println!("Total number of edges: {}", graph.edge_count());

    // Analyze using Six Degrees of Separation
    println!("\n-> Six Degrees of Separation: \n");

    // Choose 20 random movies to test
    let mut rng = rand::thread_rng();
    let movie_ids = movies
        .iter()
        .map(|movie| movie.0.to_string())
        .collect::<Vec<String>>();

    let chosen_ids = movie_ids
        .choose_multiple(&mut rng, 20)
        .cloned()
        .map(|movie_id| movie_id.as_str().to_owned())
        .collect::<Vec<String>>();
    // Loop over the list of chosen movie IDs
    for i in 0..chosen_ids.len() {
        // Parse the current ID as a usize and convert it to a node index
        let start = match chosen_ids[i].parse::<usize>() {
            // If the parsed index is valid, subtract 1 to get the correct node index
            Ok(idx) if idx <= total_nodes => NodeIndex::new(idx - 1),
            // If the parsed index is invalid, skip to the next iteration
            _ => continue,
        };

        // Loop over the remaining movie IDs
        for j in (i + 1)..chosen_ids.len() {
            // Parse the current ID as a usize and convert it to a node index
            let end = match chosen_ids[j].parse::<usize>() {
                // If the parsed index is valid, subtract 1 to get the correct node index
                Ok(idx) if idx <= total_nodes => NodeIndex::new(idx - 1),
                // If the parsed index is invalid, skip to the next iteration
                _ => continue,
            };

            // Find the shortest path between the start and end nodes using BFS
            let shortest_path = bfs(&graph, start, end);

            // If a path is found, print it out
            if let Some(path) = shortest_path {
                println!("[+] Shortest path between {} and {}:", graph[start], graph[end]);
                for node in path {
                    println!("{}", graph[node]);
                }
            } else {
                // If no path is found, print a message indicating so
                println!("[-] No path found between {} and {}", graph[start], graph[end]);
            }
        }
    }
    println!("\n\t\t\t**************\tFinished!\t**************\n");
    Ok(())
}


