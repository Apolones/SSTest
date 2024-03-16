import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CrptApi {
    private final TimeUnit refillRate;
    private final int requestLimit;
    private int tokens;
    private Queue<AbstractMap.SimpleEntry<Document, String>> queue;
    private ScheduledExecutorService scheduler;
    private final Logger LOGGER = Logger.getLogger(CrptApi.class.getName());

    public CrptApi(TimeUnit refillRate, int requestLimit) {
        this.refillRate = refillRate;
        this.requestLimit = requestLimit;
        this.tokens = 0;
        this.queue = new LinkedList<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduleRefillTask();
    }

    private void scheduleRefillTask() {
        scheduler.scheduleAtFixedRate(this::refill, 0, 1, refillRate);
    }

    private void refill() {
        tokens = requestLimit;
        processQueue();
    }

    private void processQueue() {
        Iterator<AbstractMap.SimpleEntry<Document, String>> iterator = queue.iterator();
        while (iterator.hasNext() && tokens > 0) {
            AbstractMap.SimpleEntry<Document, String> entry = iterator.next();
            createDocument(entry.getKey(), entry.getValue());
            iterator.remove();
        }
    }

    public synchronized void createDocument(Document document, String signature) {
        if (tokens > 0) {
            tokens--;
            makeRequest(document, signature);
        } else {
            queue.offer(new AbstractMap.SimpleEntry<>(document, signature));
        }
    }

    private void makeRequest(Document document, String signature) {
        String url = "https://ismp.crpt.ru/api/v3/lk/documents/create";
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(url);
            String jsonBody = mapToJson(document);
            httpPost.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
            httpPost.setHeader("Signature", signature);

            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    LOGGER.log(Level.INFO, "Response: " + entity.getContent().toString());
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error occurred while making request: " + e.getMessage());
        }
    }

    private String mapToJson(Document document) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("description", new JSONObject().put("participantInn", document.getDescription().getParticipantInn()));

            jsonObject.put("doc_id", document.getDocId());
            jsonObject.put("doc_status", document.getDocStatus());
            jsonObject.put("doc_type", document.getDocType());
            jsonObject.put("importRequest", document.isImportRequest());
            jsonObject.put("owner_inn", document.getOwnerInn());
            jsonObject.put("participant_inn", document.getParticipantInn());
            jsonObject.put("producer_inn", document.getProducerInn());
            jsonObject.put("production_date", document.getProductionDate());
            jsonObject.put("production_type", document.getProductionType());
            jsonObject.put("reg_date", document.getRegDate());
            jsonObject.put("reg_number", document.getRegNumber());

            JSONObject[] productsArray = new JSONObject[document.getProducts().length];
            for (int i = 0; i < document.getProducts().length; i++) {
                Document.Product product = document.getProducts()[i];
                JSONObject productObject = new JSONObject();
                productObject.put("certificate_document", product.getCertificateDocument());
                productObject.put("certificate_document_date", product.getCertificateDocumentDate());
                productObject.put("certificate_document_number", product.getCertificateDocumentNumber());
                productObject.put("owner_inn", product.getOwnerInn());
                productObject.put("producer_inn", product.getProducerInn());
                productObject.put("production_date", product.getProductionDate());
                productObject.put("tnved_code", product.getTnvedCode());
                productObject.put("uit_code", product.getUitCode());
                productObject.put("uitu_code", product.getUituCode());
                productsArray[i] = productObject;
            }
            jsonObject.put("products", productsArray);
        } catch (JSONException e) {
            LOGGER.log(Level.SEVERE, "Error occurred while mapping document to JSON: " + e.getMessage());
        }
        return jsonObject.toString();
    }
}
