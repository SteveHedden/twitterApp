var svg = d3.select("svg");
    width = +svg.attr("width"),
    height = +svg.attr("height");



/*
var svg = d3.select("svg");
    width = window.width,
    height = window.height;
*/

// Call zoom for svg container.
svg.call(d3.zoom().on('zoom', zoomed));

var color = d3.scaleOrdinal(d3.schemeCategory20);

var simulation = d3.forceSimulation()
    .force("link", d3.forceLink())//Or to use names rather than indices: .id(function(d) { return d.id; }))
    .force("charge", d3.forceManyBody().strength([-120]).distanceMax([500]))
    .force("center", d3.forceCenter(width / 2, height / 2));

var container = svg.append('g');

// Create form for search (see function below).
var search = d3.select("body").append('form').attr('onsubmit', 'return false;');

var box = search.append('input')
  .attr('type', 'text')
  .attr('id', 'searchTerm')
  .attr('placeholder', 'Search by @....');

var button = search.append('input')
  .attr('type', 'button')
  .attr('value', 'Search')
  .on('click', function () { searchNodes(); });

// Toggle for ego networks on click (below).
var toggle = 0;


var tooltip = d3.select("body")
  .append("div")
  .attr("class", "tooltip")
  .style("opacity", 0);


d3.json("/get-data", function(error, graph) {
  if (error) throw error;

  document.getElementById('container').innerHTML = "Click on any circle to display the tweet.";

   function topTweets(d) {
      number = d3.max(graph.nodes, function(d) {return d.tweet_id;})
      twttr.widgets.createTweet(number,
      document.getElementById('container2'),
      {
    theme: 'light',
    cards: 'hidden',
  }
  );
}
    topTweets();
  // Make object of all neighboring nodes.
  var linkedByIndex = {};
  graph.links.forEach(function(d) {
    linkedByIndex[d.source + ',' + d.target] = 1;
    linkedByIndex[d.target + ',' + d.source] = 1;
  });

  // A function to test if two nodes are neighboring.
  function neighboring(a, b) {
    return linkedByIndex[a.index + ',' + b.index];
  }

  // Linear scale for degree centrality.
  var degreeSize = d3.scaleLinear()
    .domain([d3.min(graph.nodes, function(d) {return d.degree; }),d3.max(graph.nodes, function(d) {return d.degree; })])
    .range([8,25]);

  // Collision detection based on degree centrality.
  simulation.force("collide", d3.forceCollide().radius( function (d) { return degreeSize(d.degree); }));

  var link = container.append("g")
      .attr("class", "links")
    .selectAll("line")
    .data(graph.links, function(d) { return d.source + ", " + d.target;})
    .enter().append("line")
      .attr('class', 'link');

  var node = container.append("g")
    .attr("class", "nodes")
    .selectAll("circle")
    .data(graph.nodes)
    .enter().append("circle")
    // Calculate degree centrality within JavaScript.
    //.attr("r", function(d, i) { count = 0; graph.links.forEach(function(l) { if (l.source == i || l.target == i) { count += 1;}; }); return size(count);})
    // Use degree centrality from NetworkX in json.
    .attr('r', function(d) { return degreeSize(d.degree); })
    // Color by group, a result of modularity calculation in NetworkX.
    .attr("fill", function(d) { return color(d.group); })
    .attr('class', 'node')
    .text(function(d) { return d.name; })
  .on('mouseover', function(d, i) {
    d3.select(this)
      .transition()
      .duration(100)
      .attr('r', function(d) { return degreeSize(d.degree)*2; });
    //.text(function(d) { return d.name; });
  })
   .on('mouseout', function(d, i) {
      d3.select(this)
        .transition()
        .duration(100)
      .attr('r', function(d) { return degreeSize(d.degree); })
    })
  .on('mouseover.tooltip', function(d) {
      tooltip.transition()
        .duration(300)
        .style("opacity", .8);
      tooltip.html("<b>" + "Name: " + "</b>" + d.name + "<p/>group:" + d.tweet_text)
        .style('font-size', '16px')
        .style('font-family', 'Helvetica')
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY + 10) + "px");
    })
    .on("mouseout.tooltip", function() {
        tooltip.transition()
          .duration(100)
          .style("opacity", 0);
      })
      // On click, toggle ego networks for the selected node.
      .on('click', function(d, i) {
        document.getElementById('container').innerHTML = "";
        number = d.tweet_id
        console.log(number);
        twttr.widgets.createTweet(number,
          document.getElementById('container'),
          {
            theme: 'light'
          }
        );
              if (toggle == 0) {
          // Ternary operator restyles links and nodes if they are adjacent.
          d3.selectAll('.link').style('stroke-opacity', function (l) {
            return l.target == d || l.source == d ? 1 : 0.1;
          });
          d3.selectAll('.node').style('opacity', function (n) {
            return neighboring(d, n) ? 1 : 0.1;
          });
          d3.select(this).style('opacity', 1);
          toggle = 1;
        }
        else {
          // Restore nodes and links to normal opacity.
          d3.selectAll('.link').style('stroke-opacity', '0.6');
          d3.selectAll('.node').style('opacity', '1');
          toggle = 0;
        }
      })
      .call(d3.drag()
          .on("start", dragstarted)
          .on("drag", dragged)
          .on("end", dragended));

  var lables = node.append("text")
      .text(function(d) {
        return d.name;
      })
      .attr('x', 6)
      .attr('y', 3);

  simulation
      .nodes(graph.nodes)
      .on("tick", ticked);

  simulation.force("link")
      .links(graph.links);

  function ticked() {
    link
        .attr("x1", function(d) { return d.source.x; })
        .attr("y1", function(d) { return d.source.y; })
        .attr("x2", function(d) { return d.target.x; })
        .attr("y2", function(d) { return d.target.y; });

    node
        .attr("cx", function(d) { return d.x; })
        .attr("cy", function(d) { return d.y; });
  }

  function mouseover() {
    d3.select(this).select("circle").transition()
        .duration(750)
        .attr("r", 16);
  }

  function mouseout() {
    d3.select(this).select("circle").transition()
        .duration(750)
        .attr("r", 8);
  }

});

function dragstarted(d) {
  if (!d3.event.active) simulation.alphaTarget(0.3).restart();
  d.fx = d.x;
  d.fy = d.y;
}

function dragged(d) {
  d.fx = d3.event.x;
  d.fy = d3.event.y;
}

function dragended(d) {
  if (!d3.event.active) simulation.alphaTarget(0);
  d.fx = null;
  d.fy = null;
}

// Zooming function translates the size of the svg container.
function zoomed() {
    container.attr("transform", "translate(" + d3.event.transform.x + ", " + d3.event.transform.y + ") scale(" + d3.event.transform.k + ")");
}

// Search for nodes by making all unmatched nodes temporarily transparent.
function searchNodes() {
  var term = document.getElementById('searchTerm').value;
  var selected = container.selectAll('.node').filter(function (d, i) {
    return d.name.toLowerCase().search(term.toLowerCase()) == -1;
  });
  selected.style('opacity', '0');
  var link = container.selectAll('.link');
  link.style('stroke-opacity', '0');
  d3.selectAll('.node').transition()
    .duration(5000)
    .style('opacity', '1');
  d3.selectAll('.link').transition().duration(5000).style('stroke-opacity', '0.6');
}