package handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

import helio.blueprints.DataHandler;


public class JsonHandler implements DataHandler {
	
	private static final Gson GSON = new Gson();
	private String iterator;
	Logger logger = LoggerFactory.getLogger(JsonHandler.class);
	private static final String CONFIGURATION_KEY = "iterator";

	/**
	 * This constructor creates an empty {@link JsonHandler} that will need to be configured using a valid {@link JsonObject}
	 */
	public JsonHandler() {
		super();
	}

	/**
	 * This constructor instantiates a valid {@link JsonHandler} with the provided iterator
	 * @param iterator a valid Json Path expression
	 */
	public JsonHandler(String iterator) {
		this.iterator = iterator;
	}

	public String getIterator() {
		return iterator;
	}

	public void setIterator(String iterator) {
		this.iterator = iterator;
	}

	@Override
	public List<String> iterator(String dataChunk) {
		List<String> queueOfresults = new ArrayList<>();
		if(dataChunk!=null) {
			Configuration conf =  Configuration.defaultConfiguration()
												.addOptions(Option.ALWAYS_RETURN_LIST)
												.addOptions(Option.REQUIRE_PROPERTIES)
												.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
			try {
				List<Object> results = JsonPath.using(conf).parse(dataChunk).read(iterator);
				for (Object result : results) {
					if(result instanceof Map) {
						@SuppressWarnings("unchecked")
						Map<String,String> map = (Map<String,String>) result;
						if( map != null && !map.isEmpty())
							queueOfresults.add(GSON.toJson(map));
					}else {
						queueOfresults.add(result.toString());
					}
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
		}
		return queueOfresults;
	}

	@SuppressWarnings("unchecked")
	public List<String> filter(String filter, String dataChunk) {

		Configuration conf = Configuration.defaultConfiguration()
											.addOptions(Option.REQUIRE_PROPERTIES)
											.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
		List<String> results = new ArrayList<>();
		try {
			Object parsed = JsonPath.using(conf).parse(dataChunk).read(filter);
			if(parsed!=null) {
				if (parsed instanceof Collection) {
					List<Object> resultsAux = (List<Object>) parsed;
					resultsAux.stream().filter(elem -> elem!=null).forEach(elem -> toSimpleResult(elem, results));
				}/*else if(parsed instanceof Map) {
					Map<String,String> map = (Map<String,String>) parsed;
					if( map != null && !map.isEmpty())
						results.add(GSON.toJson(map));
				}else {
					results.add(String.valueOf(parsed));
				}*/
				else {
					toSimpleResult(parsed, results);
				}
			}
		}catch(Exception e) {
			logger.error(e.toString());
		}

		return results;
	}

	private void toSimpleResult(Object parsed, List<String> results) {
		if(parsed instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String,String> map = (Map<String,String>) parsed;
			if( map != null && !map.isEmpty())
				results.add(GSON.toJson(map));
		}else {
			results.add(String.valueOf(parsed));
		}
	}
	
	@Override
	public void configure(JsonObject arguments) {
		if(arguments.has(CONFIGURATION_KEY)) {
			iterator = arguments.get(CONFIGURATION_KEY).getAsString();
			if(iterator.isEmpty())
				throw new IllegalArgumentException("JsonHandler needs to receive non empty value for the keey 'iterator'");
		}else {
			throw new IllegalArgumentException("JsonHandler needs to receive json object with the mandatory key 'iterator'");
		}
		
	}

}
