import AppKit
import Foundation
import Vision

struct OCRItem: Codable {
    let id: Int
    let text: String
    let confidence: Double
    let bbox: [[Double]]
}

func rectPoints(_ rect: CGRect, width: Double, height: Double) -> [[Double]] {
    let x1 = Double(rect.minX) * width
    let x2 = Double(rect.maxX) * width
    let y1 = (1.0 - Double(rect.maxY)) * height
    let y2 = (1.0 - Double(rect.minY)) * height
    return [
        [x1, y1],
        [x2, y1],
        [x2, y2],
        [x1, y2],
    ]
}

func loadCGImage(from path: String) -> CGImage? {
    guard let image = NSImage(contentsOfFile: path) else {
        return nil
    }
    return image.cgImage(forProposedRect: nil, context: nil, hints: nil)
}

guard CommandLine.arguments.count >= 2 else {
    FileHandle.standardError.write(Data("usage: apple_vision_ocr <image>\n".utf8))
    exit(2)
}

let imagePath = CommandLine.arguments[1]
guard let cgImage = loadCGImage(from: imagePath) else {
    FileHandle.standardError.write(Data("failed_to_load_image\n".utf8))
    exit(3)
}

let width = Double(cgImage.width)
let height = Double(cgImage.height)
var requestError: Error?
var observations: [VNRecognizedTextObservation] = []

let request = VNRecognizeTextRequest { request, error in
    requestError = error
    observations = (request.results as? [VNRecognizedTextObservation]) ?? []
}
request.recognitionLevel = .accurate
request.usesLanguageCorrection = false

do {
    let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
    try handler.perform([request])
} catch {
    FileHandle.standardError.write(Data("vision_request_failed:\(error)\n".utf8))
    exit(4)
}

if let requestError {
    FileHandle.standardError.write(Data("vision_request_failed:\(requestError)\n".utf8))
    exit(5)
}

let sortedObservations = observations.sorted { left, right in
    let yDiff = abs(left.boundingBox.maxY - right.boundingBox.maxY)
    if yDiff > 0.01 {
        return left.boundingBox.maxY > right.boundingBox.maxY
    }
    return left.boundingBox.minX < right.boundingBox.minX
}

var items: [OCRItem] = []
var nextID = 1

for observation in sortedObservations {
    guard let candidate = observation.topCandidates(1).first else {
        continue
    }
    let fullText = candidate.string.trimmingCharacters(in: .whitespacesAndNewlines)
    if fullText.isEmpty {
        continue
    }

    var emitted = false
    var searchStart = candidate.string.startIndex

    for part in candidate.string.split(whereSeparator: { $0.isWhitespace }) {
        let token = String(part).trimmingCharacters(in: .whitespacesAndNewlines)
        if token.isEmpty {
            continue
        }
        guard let range = candidate.string.range(
            of: token,
            options: [],
            range: searchStart..<candidate.string.endIndex
        ) else {
            continue
        }
        searchStart = range.upperBound

        guard let rectObservation = try? candidate.boundingBox(for: range) else {
            continue
        }

        items.append(
            OCRItem(
                id: nextID,
                text: token,
                confidence: Double(candidate.confidence),
                bbox: rectPoints(rectObservation.boundingBox, width: width, height: height)
            )
        )
        nextID += 1
        emitted = true
    }

    if emitted {
        continue
    }

    items.append(
        OCRItem(
            id: nextID,
            text: fullText,
            confidence: Double(candidate.confidence),
            bbox: rectPoints(observation.boundingBox, width: width, height: height)
        )
    )
    nextID += 1
}

let encoder = JSONEncoder()
encoder.outputFormatting = [.sortedKeys]
do {
    let payload = try encoder.encode(items)
    FileHandle.standardOutput.write(payload)
} catch {
    FileHandle.standardError.write(Data("encode_failed:\(error)\n".utf8))
    exit(6)
}
