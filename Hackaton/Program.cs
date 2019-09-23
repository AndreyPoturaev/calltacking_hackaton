using BAMCIS.PrestoClient;
using BAMCIS.PrestoClient.Interfaces;
using BAMCIS.PrestoClient.Model;
using BAMCIS.PrestoClient.Model.Statement;
using F23.StringSimilarity;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using TinyCsvParser;
using TinyCsvParser.Mapping;

namespace Hackaton
{
    class Program
    {


        static async System.Threading.Tasks.Task Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var loader = serviceProvider.GetService<Loader>();

            //await loader.LoadAnnouncementsInfoAsync();
            //await loader.LoadHouseInfoAsync();

            var flow = serviceProvider.GetService<Flow>();


            await flow.RunAsync();

            Console.ReadLine();
        }



        private static void ConfigureServices(IServiceCollection services)
        {
            services.AddLogging(configure => configure.AddConsole())
                .Configure<LoggerFilterOptions>(options => options.MinLevel = LogLevel.Trace)
                .AddTransient<Analyzer>()
                .AddTransient<Loader>()
                .AddTransient<Flow>()
                .AddTransient<Saver>();
        }
    }
}
