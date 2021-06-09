package org.apache.lucene.facet.range;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.RandomSamplingFacetsCollector;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

/**
 * {@link Facets} implementation that computes counts for dynamic long ranges.
 *
 * @lucene.experimental
 */
public class DynamicLongRangeFacetCounts extends LongRangeFacetCounts {

    /**
     * Create 100 temporary bins to hold the estimated equi-probable bins from 1000 samples.
     */
    private int Top_N_BINS = 100;
    /**
     * The number of samples we want from the data.
     */
    private int SAMPLE_SIZE = 1000;

    /**
     * Create {@code DynamicLongRangeFacetCounts} using long values from the specified field. The field may
     * be single-valued ({@link NumericDocValues}) or multi-valued ({@link SortedNumericDocValues}),
     * and will be interpreted as containing long values.
     */
    public DynamicLongRangeFacetCounts(String field, FacetsCollector hits, IndexSearcher searcher, Query q) throws IOException {
        this(field, LongValuesSource.fromLongField(field), hits, searcher, q);
    }

    /**
     * Create {@code DynamicLongRangeFacetCounts}, using the provided {@link LongValuesSource} if non-null.
     * If {@code valueSource} is null, doc values from the provided {@code field} will be used.
     */
    public DynamicLongRangeFacetCounts(String field, LongValuesSource valueSource, FacetsCollector hits, IndexSearcher searcher, Query q) throws IOException {
        this(field, valueSource, hits, null, searcher, q);
    }

    /**
     * Create {@code DynamicLongRangeFacetCounts} using long values from the specified field. The field may
     * be single-valued ({@link NumericDocValues}) or multi-valued ({@link SortedNumericDocValues}),
     * and will be interpreted as containing long values.
     */
    public DynamicLongRangeFacetCounts(String field, LongValuesSource valueSource, FacetsCollector hits, Query fastMatchQuery, IndexSearcher searcher, Query q) throws IOException {
        super(field, fastMatchQuery);
        this.ranges = computeRanges(searcher, q, field);
        this.counts = new int[ranges.length];
        // pass counts and ranges array to the count function in LongRangeFacetCounts. Fill the counts array, get the number of counts for each bin
        count(valueSource, hits.getMatchingDocs());
    }

    private LongRange[] computeRanges(IndexSearcher searcher, Query q, String field) throws IOException {
        Random random = new Random();
        // getting random samples
        RandomSamplingFacetsCollector samples =
                new RandomSamplingFacetsCollector(SAMPLE_SIZE, random.nextLong());

        searcher.search(q, samples);

        List<MatchingDocs> matchingDocs = samples.getMatchingDocs();

//        int matchingTotalHits = matchingDocs.stream().mapToInt(doc -> doc.totalHits).sum();
        int matchingTotalHits = 0;

        for (MatchingDocs doc : matchingDocs) {
            matchingTotalHits += doc.totalHits;
        }

        // Adding two additional bins for lower outliers and for upper outliers.
        LongRange[] ranges = new LongRange[Math.min(Top_N_BINS, matchingTotalHits / 10) + 2];
        Long[] prices = new Long[Math.min(SAMPLE_SIZE, matchingTotalHits)];
        int i = 0;

        for (MatchingDocs hits : matchingDocs) {
            final DocIdSetIterator it = hits.bits.iterator();
            if (it == null) {
                continue;
            }

            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
                prices[i++] = Long.parseLong(searcher.doc(doc).getField(field).stringValue());
                doc = it.nextDoc();
            }
        }

        Arrays.sort(prices);
        int countPerBin = prices.length / (ranges.length - 2);
        for (int priceIdx = 0; priceIdx < prices.length; priceIdx += countPerBin) {
            // +1 because the first bucket is reserved for the outliers
            int binNumber = (priceIdx / countPerBin) + 1;
            // name, lower-bound, true, upperbound,
            ranges[binNumber] = new LongRange(String.format("Dynamic_range_%d", binNumber),  prices[priceIdx], true, prices[priceIdx + countPerBin - 1], true);
        }
        ranges[0] = new LongRange("Dynamic_range_min",  Long.MIN_VALUE, true, prices[0], false);
        ranges[ranges.length - 1] = new LongRange("Dynamic_range_max", prices[prices.length-1], false, Long.MAX_VALUE, true);

        return ranges;
    }

//    @Override
//    protected LongRange[] getLongRanges() {
//        return (LongRange[]) this.ranges;
//    }
}
