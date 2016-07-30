package com.palvair.elasticsearch.java.api;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import lombok.Data;

public class Application {

	private static Client client;

	private static final String indice = "tools";

	static {
		try {
			Settings settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch_widdy").build();
			client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		} catch (UnknownHostException e) {
			client = null;
		}
	}

	public static void main(String[] args) throws UnknownHostException, InterruptedException {
		cleanIndice();

		populate();

		SearchResponse response = client.prepareSearch(indice).setQuery(QueryBuilders.matchAllQuery()).execute()
				.actionGet();

		for (SearchHit hit : response.getHits()) {
			System.out.println("hit found = " + hit.getSourceAsString());
		}
	}

	public static IndexResponse insert(Tool tool) {
		try {
			final IndexResponse indexResponse = client
					.prepareIndex(indice, tool.getType()).setSource(jsonBuilder().startObject()
							.field("name", tool.getName()).field("format", tool.getFormat()).endObject())
					.setRefresh(true).get();
			return indexResponse;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static DeleteIndexResponse cleanIndice() {
		return client.admin().indices().delete(new DeleteIndexRequest(indice)).actionGet();
	}

	public static List<IndexResponse> populate() throws InterruptedException {
		IndexResponse es = insert(new Application.Tool("elasticsearch", "data", "json"));
		IndexResponse hadoop = insert(new Application.Tool("hadoop", "data", "na"));
		return Arrays.asList(es, hadoop);
	}

	public static long getTotalHits() {
		SearchResponse countResponse = client.prepareSearch(indice).setSize(0).setQuery(QueryBuilders.matchAllQuery())
				.execute().actionGet();
		return countResponse.getHits().getTotalHits();
	}

	@Data
	public static class Tool {
		private final String name;
		private final String type;
		private final String format;

		public Tool(String name, String type, String format) {
			this.name = name;
			this.type = type;
			this.format = format;
		}
	}
}
