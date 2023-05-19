import React, { useRef, useEffect } from "react";
import * as d3 from "d3";

const NetworkVisualization = ({ data }) => {
	const svgRef = useRef(null);

	useEffect(() => {
		const nodes = data.reduce((acc, item) => {
			acc.push({ id: item.user, group: "User", radius: 2 });
			item.recommended_users.forEach((user) => {
				acc.push({
					id: user.node,
					group: "Recommended Users",
				});
			});
			return acc;
		}, []);

		const links = data.reduce((acc, item) => {
			item.recommended_users.forEach((user) => {
				acc.push({
					source: item.user,
					target: user.node,
					authority: user.authority,
				});
			});
			return acc;
		}, []);

		console.log(nodes);

		const width = 800;
		const height = 600;
		const radius = 5;

		const simulation = d3
			.forceSimulation()
			.force(
				"link",
				d3.forceLink().id((d) => d.id)
			)
			.force("charge", d3.forceManyBody())
			.force("center", d3.forceCenter(width / 2, height / 2));

		const svg = d3
			.select(svgRef.current)
			.append("svg")
			.attr("viewBox", [0, 0, width, height]);

		const link = svg
			.append("g")
			.attr("class", "links")
			.selectAll("line")
			.data(links)
			.enter()
			.append("line")
			//.attr("stroke-width", (d) => Math.sqrt(d.authority))
			.attr("stroke-width", (d) => (d.authority*1000))
			.attr("stroke", "#fff")
			.attr("stroke-opacity", ".9");

		const node = svg
			.append("g")
			.attr("class", "nodes")
			.selectAll("circle")
			.data(nodes)
			.enter()
			.append("circle")
			.attr("r", radius)
			//.attr("fill", "#59E692")
			.attr("fill", (d) => color(d.group))
			.call(drag(simulation))
			//.on("mouseover", handleMouseOver)
			//.on("mouseout", handleMouseOut);

		const ticked = () => {
			link.attr("x1", (d) => d.source.x)
				.attr("y1", (d) => d.source.y)
				.attr("x2", (d) => d.target.x)
				.attr("y2", (d) => d.target.y);

			node.attr("cx", (d) => d.x).attr("cy", (d) => d.y);
		};

		simulation.nodes(nodes).on("tick", ticked);
		simulation.force("link").links(links);

		function color(group) {
			const scale = d3.scaleOrdinal(d3.schemeCategory10);
			return scale(group);
		}

		function drag(simulation) {
			function dragstarted(event) {
				if (!event.active) simulation.alphaTarget(0.3).restart();
				event.subject.fx = event.subject.x;
				event.subject.fy = event.subject.y;
			}

			function dragged(event) {
				event.subject.fx = event.x;
				event.subject.fy = event.y;
			}

			function dragended(event) {
				if (!event.active) simulation.alphaTarget(0);
				event.subject.fx = null;
				event.subject.fy = null;
			}

			return d3
				.drag()
				.on("start", dragstarted)
				.on("drag", dragged)
				.on("end", dragended);
		}
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
