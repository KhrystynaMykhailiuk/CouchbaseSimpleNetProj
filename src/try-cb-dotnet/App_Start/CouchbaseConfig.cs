using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Couchbase;
using Couchbase.Configuration.Client;
using Couchbase.Authentication;
using Couchbase.Core;

namespace try_cb_dotnet
{
    public static class CouchbaseConfig
    {
        private static readonly List<string> TravelSampleIndexNames = new List<string>
        {
            "def_sourceairport",
            "def_airportname",
            "def_type",
            "def_faa",
            "def_icao",
            "def_city"
        };

        public static void Register()
        {
            var couchbaseServer = ConfigurationManager.AppSettings.Get("CouchbaseServer");
            var username = ConfigurationManager.AppSettings.Get("CouchbaseUser");
            var password = ConfigurationManager.AppSettings.Get("CouchbasePassword");
            var bucketName = ConfigurationManager.AppSettings.Get("CouchbaseTravelBucket");

            var cluster = new Cluster(new ClientConfiguration
            {
                Servers = new List<Uri> { new Uri("http://localhost:8091") }
            });

            var authenticator = new PasswordAuthenticator(username, password);
            cluster.Authenticate(authenticator);
            ClusterHelper.Initialize();
            var bucket = cluster.OpenBucket(bucketName, password);


            //var config = new ClientConfiguration
            //{
            //    Servers = new List<Uri>() { new Uri("http://localhost:8091/") },
            //    UseSsl = true,
            //    DefaultOperationLifespan = 1000,
            //    BucketConfigs = new Dictionary<string, BucketConfiguration>
            //    {
            //         {"travel-sample", new BucketConfiguration
            //           {
            //                BucketName = "travel-sample",
            //                UseSsl = false,
            //                Password = "P@ssw0rd",
            //                DefaultOperationLifespan = 2000,
            //                PoolConfiguration = new PoolConfiguration
            //                {
            //                    MaxSize = 10,
            //                    MinSize = 5,
            //                    SendTimeout = 12000
            //                }
            //          }
            //        }
            //    }
            //};


            //using (var cluster_ = new Cluster(config))
            //{
            //    IBucket bucket_ = null;
            //    try
            //    {
            //        //bucket_ = cluster.OpenBucket();
            //        bucket_ = cluster.OpenBucket(bucketName, password);
            //        //use the bucket here
            //    }
            //    finally
            //    {
            //        if (bucket_ != null)
            //        {
            //            cluster.CloseBucket(bucket_);
            //        }
            //    }
            //}

            EnsureIndexes(bucketName, username, password);
        }

        private static void EnsureIndexes(string bucketName, string username, string password)
        {
            var bucket = ClusterHelper.GetBucket(bucketName, password);
            var bucketManager = bucket.CreateManager(username, password);

            var indexes = bucketManager.ListN1qlIndexes();
            if (!indexes.Any(index => index.IsPrimary))
            {
                bucketManager.CreateN1qlPrimaryIndex(true);
            }

            var missingIndexes = TravelSampleIndexNames.Except(indexes.Where(x => !x.IsPrimary).Select(x => x.Name)).ToList();
            if (!missingIndexes.Any())
            {
                return;
            }

            foreach (var missingIndex in missingIndexes)
            {
                var propertyName = missingIndex.Replace("def_", string.Empty);
                bucketManager.CreateN1qlIndex(missingIndex, true, propertyName);
            }

            bucketManager.BuildN1qlDeferredIndexes();
            bucketManager.WatchN1qlIndexes(missingIndexes, TimeSpan.FromSeconds(30));
        }

        public static void CleanUp()
        {
            ClusterHelper.Close();
        }
    }
}