import React, { useRef, useEffect } from "react";
import * as d3 from "d3";
//import './NetworkVisualization.css';

const NetworkVisualization = ({ data }) => {
	const svgRef = useRef(null);

	useEffect(() => {

		const nodes = data.reduce((acc, item) => {
			acc.push({ id: item.user, group: "User", radius: 2 });
			item.recommended_users.forEach((user) => {
				acc.push({
					id: user.node,
					group: "Recommended Users",
					radius: 2,
				});
			});
			return acc;
		}, []);

		const links = data.reduce((acc, item) => {
			item.recommended_users.forEach((user) => {
				acc.push({
					source: item.user,
					target: user.node,
					value: user.authority,
				});
			});
			return acc;
		}, []);
	
		console.log(nodes)
		const svg = d3.select(svgRef.current);

		// Define the dimensions of the SVG canvas
		const width = +svg.attr("width");
		const height = +svg.attr("height");

		// Create a force simulation to lay out the nodes and links
		const simulation = d3
			.forceSimulation()
			.force(
				"link",
				d3.forceLink().id((d) => d.id)
			)
			.force("charge", d3.forceManyBody())
			.force("center", d3.forceCenter(width / 2, height / 2));

		// Add the links to the SVG canvas
		const link = svg
			.append("g")
			.attr("class", "links")
			.selectAll("line")
			.data(links)
			.enter()
			.append("line")
			.attr("stroke-width", (d) => Math.sqrt(d.value));

		// Add the nodes to the SVG canvas
		const node = svg
			.append("g")
			.attr("class", "nodes")
			.selectAll("circle")
			.data(nodes)
			.enter()
			.append("circle")
			.attr("r", 5)
			.attr("fill", "#69b3a2");

		// Define the tick function to update the position of the nodes and links
		const ticked = () => {
			link.attr("x1", (d) => d.source.x)
				.attr("y1", (d) => d.source.y)
				.attr("x2", (d) => d.target.x)
				.attr("y2", (d) => d.target.y);

			node.attr("cx", (d) => d.x).attr("cy", (d) => d.y);
		};

		// Run the simulation and update the positions of the nodes and links on each tick
		simulation.nodes(nodes).on("tick", ticked);
		simulation.force("link").links(links);
	}, [data]);

	return (
		<svg
			ref={svgRef}
			className="network-visualization"
			width="800"
			height="600"
		></svg>
	);
};

export default NetworkVisualization;
