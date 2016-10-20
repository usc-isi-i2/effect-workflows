from APIDownloader import APIDownloader
from argparse import ArgumentParser


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("-o", "--output", type=str, help="Output filename", required=True)
    parser.add_argument("-p", "--password", type=str, help="password for connecting to hyperion gray api", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    url = "https://effect.hyperiongray.com/api/leaked-source/email?query=*@"
    domains = [ "alaska.edu",
                "apple.afsmith.bm",
                "bremertonhousing.org",
                "clixsense.com",
                "Empireminecraft.com",
                "eurekalert.org",
                "feverclan.com",
                "floridabar.org",
                "i-dressup.com",
                "jivesoftware.com",
                "justformen.com",
                "Last.fm",
                "manaliveinc.org",
                "newseasims.com",
                "saintfrancis.com",
                "ssctech.com",
                "unm.edu",
                "usc.edu",
                "wpcapital.com"
            ]

    apiDownloader = APIDownloader()
    out_file = open(args.output, 'w')

    for domain in domains:
        results = apiDownloader.download_api(url + domain, "isi", args.password)
        if results is not None:
            if "results" in results:
                apiDownloader.write_as_json_lines(results["results"], out_file)

    out_file.close()

