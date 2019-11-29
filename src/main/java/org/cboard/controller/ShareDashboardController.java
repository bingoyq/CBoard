package org.cboard.controller;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import org.cboard.dao.DatasetDao;
import org.cboard.dataprovider.config.AggConfig;
import org.cboard.dataprovider.result.AggregateResult;
import org.cboard.dto.ViewAggConfig;
import org.cboard.dto.ViewDashboardBoard;
import org.cboard.dto.ViewDashboardDataset;
import org.cboard.pojo.DashboardDataset;
import org.cboard.services.BoardService;
import org.cboard.services.DataProviderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/shareDashboard")
public class ShareDashboardController extends BaseController{

    @Autowired
    private DataProviderService dataProviderService;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private BoardService boardService;

    @RequestMapping(value = "/getBoardData")
    public ViewDashboardBoard getBoardData(@RequestParam(name = "id") Long id) {
        return boardService.getBoardData(id);
    }

    @RequestMapping(value = "/getDatasetList")
    public List<ViewDashboardDataset> getDatasetList() {
        List<DashboardDataset> list = datasetDao.getDatasetList(tlUser.get().getUserId());
        return Lists.transform(list, ViewDashboardDataset.TO);
    }

    @RequestMapping(value = "/getAggregateData")
    public AggregateResult getAggregateData(@RequestParam(name = "datasourceId", required = false) Long datasourceId,
                                            @RequestParam(name = "query", required = false) String query,
                                            @RequestParam(name = "datasetId", required = false) Long datasetId,
                                            @RequestParam(name = "cfg") String cfg,
                                            @RequestParam(name = "reload", required = false, defaultValue = "false") Boolean reload) {
        Map<String, String> strParams = null;
        if (query != null) {
            JSONObject queryO = JSONObject.parseObject(query);
            strParams = Maps.transformValues(queryO, Functions.toStringFunction());
        }
        AggregateResult aggResult = null;
        // data source aggreagtor instance need not lock
        boolean isDataSourceAggInstance = dataProviderService.isDataSourceAggInstance(datasourceId, strParams, datasetId);
        String randomFlag = isDataSourceAggInstance ? UUID.randomUUID().toString() : "1";
        String lockString = Hashing.md5().newHasher()
                .putString(datasourceId + query + datasetId + tlUser.get().getUserId() + randomFlag, Charsets.UTF_8)
                .hash().toString();
        synchronized (lockString.intern()) {
            AggConfig config = ViewAggConfig.getAggConfig(JSONObject.parseObject(cfg, ViewAggConfig.class));
            aggResult = dataProviderService.queryAggData(datasourceId, strParams, datasetId, config, reload);
        }
        return aggResult;
    }
}
