using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using static Hackaton.Loader;

namespace Hackaton
{
    public class Flow
    {
        public Flow(ILogger<Flow> logger, Loader loader, Analyzer analyzer, Saver saver)
        {
            _logger = logger;
            _loader = loader;
            _analyzer = analyzer;
            _saver = saver;    
        }


        private readonly ILogger _logger;
        private readonly Loader _loader;
        private readonly Analyzer _analyzer;
        private readonly Saver _saver;

        public async Task RunAsync()
        {
            CallsInfo[] data;

            if (GlobalSettings.UseExperimentalDump)
                data = await _saver.LoadExperimentalDumpAsync();
            else
            {
                data = await _loader.LoadDataAsync();

                await _loader.LoadTextsAsync(data);

                await _loader.LoadAnnouncementsAsync(data);

                await _loader.LoadAnnouncementsStreetsAsync(data);

                await _loader.LoadAnnouncementsHousesAsync(data);

                await _loader.LoadAnnouncementsPhoneViewInfoAsync(data);

                await _saver.SaveExperimentalDumpAsync(data);
            }

            await _analyzer.AnalyzeThisAsync(data);

            await _saver.SaveResultAsync(data);

            await _saver.SaveDumpAsync(data);
        }
    }
}
