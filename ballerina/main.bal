import ballerina/io;
import ballerina/lang.regexp;
import ballerinax/azure_storage_service.files as azure_files;

configurable string SAS = ?;
configurable string accountName = ?;

const CHUNK = 4 * 1024 * 1024; // 4 MiB

azure_files:ConnectionConfig fileServiceConfig = {
    accessKeyOrSAS: SAS,
    accountName: accountName,
    authorizationMethod: "SAS"
};

azure_files:FileClient fileClient = check new (fileServiceConfig);

public function main() returns error? {
    string share = "testf1";
    string dir   = "test-smb";
    string name  = "file-10mb.txt";

    check streamAzureFileLinesSizeAgnostic(fileClient, share, name, dir);
    io:println("Logging Completed!");
}

// --- Stream-read and print lines ---
function streamAzureFileLinesSizeAgnostic(azure_files:FileClient fileClient, string share, string name, string? dir, int maxLines = -1 ) returns error? {
    final regexp:RegExp lineBreak = re `\r?\n`;
    final regexp:RegExp NL        = re `\n`;

    int offset = 0;
    string carry = "";
    int printed = 0;

    while true {
        azure_files:ContentRange r = { startByte: offset, endByte: offset + CHUNK - 1 };
        byte[]|azure_files:Error chunk = fileClient->getFileAsByteArray(share, name, dir, r);

        if chunk is error {
            // The connector returns either azure_files:ProcessingError or http:ClientError.
            if chunk is azure_files:ServerError {
                // Inspect the detail record for status code / error code.
                anydata det = chunk.detail();
                int? sc = det is map<anydata> ? <int?>det["statusCode"] : ();
                string? code = det is map<anydata> ? <string?>det["errorCode"] : ();

                // Treat 416 (Invalid Range) as EOF when file size is an exact multiple of CHUNK.
                if sc == 416 || code == "InvalidRange" {
                    break;
                }
            }
            // Anything else: bubble the error.
            return chunk;
        }

        if chunk.length() == 0 {
            break;
        }

        string text = carry + check string:fromBytes(chunk);
        string[] parts = lineBreak.split(text);

        boolean endsWithNewline =
            text.length() > 0 && (NL.matchAt(text, text.length() - 1) is regexp:Span);
        int emitUpto = endsWithNewline ? parts.length() : parts.length() - 1;

        foreach int i in 0 ..< emitUpto {
            io:println(parts[i]);
            printed += 1;
            if maxLines > 0 && printed >= maxLines {
                return;
            }
        }

        carry = endsWithNewline ? "" : parts[parts.length() - 1];
        offset += chunk.length();

        // If we got a short block, that was the last chunk.
        if chunk.length() < CHUNK {
            break;
        }
    }

    if carry.length() > 0 {
        io:println(carry);
    }
}
