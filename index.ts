import youtube from '@googleapis/youtube';
import fs from 'fs/promises';
import dotenv from 'dotenv';
import { parse } from 'csv-parse';
import { stringify } from 'csv-stringify/sync';
import he from 'he';

dotenv.config();

const youtubeApi = youtube.youtube({
    version: 'v3',
    auth: process.env.YOUTUBE_API_KEY
});

interface VideoRecord {
    ID: string;
    Title: string;
    'Published At': string;
    Duration: string;
    ViewCount?: number;
    LikeCount?: number;
}

async function getNewVideos(channelId: string, existingIds: Set<string>): Promise<VideoRecord[]> {
    const newVideos: VideoRecord[] = [];
    let pageToken: string | undefined;
    let totalApiCalls = 0;
    
    try {
        do {
            totalApiCalls++;
            const response = await youtubeApi.search.list({
                part: ['id', 'snippet'],
                channelId: channelId,
                maxResults: 50,
                order: 'date',
                type: ['video'],
                pageToken: pageToken
            });

            const items = response.data.items || [];
            
            for (const item of items) {
                if (item.id?.videoId && !existingIds.has(item.id.videoId)) {
                    newVideos.push({
                        ID: item.id.videoId,
                        Title: item.snippet?.title || '',
                        'Published At': item.snippet?.publishedAt || '',
                        Duration: '' // Will be filled by videos.list call
                    });
                }
            }

            pageToken = response.data.nextPageToken || undefined;
            console.log(`Checked ${items.length} videos, found ${newVideos.length} new ones so far...`);
            
        } while (pageToken);

        console.log(`Made ${totalApiCalls} search.list calls (${totalApiCalls * 100} units)`);

        // Get full details including duration for new videos
        if (newVideos.length > 0) {
            for (let i = 0; i < newVideos.length; i += 50) {
                const chunk = newVideos.slice(i, i + 50);
                const response = await youtubeApi.videos.list({
                    part: ['contentDetails', 'statistics'],
                    id: chunk.map(v => v.ID)
                });

                response.data.items?.forEach(item => {
                    const video = newVideos.find(v => v.ID === item.id);
                    if (video && item.contentDetails) {
                        video.Duration = item.contentDetails.duration || '';
                        video.ViewCount = Number(item.statistics?.viewCount) || 0;
                        video.LikeCount = Number(item.statistics?.likeCount) || 0;
                    }
                });
                
                console.log(`Got full details for videos ${i + 1} to ${i + chunk.length} of ${newVideos.length}`);
            }
        }

        return newVideos;
    } catch (error) {
        console.error('Error fetching new videos:', error);
        throw error;
    }
}

async function updateExistingVideos(videos: VideoRecord[]): Promise<VideoRecord[]> {
    try {
        // Update in chunks of 50 to respect API limits
        for (let i = 0; i < videos.length; i += 50) {
            const chunk = videos.slice(i, i + 50);
            const response = await youtubeApi.videos.list({
                part: ['statistics', 'contentDetails'],
                id: chunk.map(v => v.ID)
            });

            response.data.items?.forEach(item => {
                const video = videos.find(v => v.ID === item.id);
                if (video && item.statistics) {
                    video.ViewCount = Number(item.statistics.viewCount) || 0;
                    video.LikeCount = Number(item.statistics.likeCount) || 0;
                }
            });

            console.log(`Updated metadata for videos ${i + 1} to ${i + chunk.length} of ${videos.length}`);
        }

        return videos;
    } catch (error) {
        console.error('Error updating existing videos:', error);
        throw error;
    }
}

async function readCsvFile(filePath: string): Promise<VideoRecord[]> {
    try {
        const fileContent = await fs.readFile(filePath, 'utf-8');
        return new Promise((resolve, reject) => {
            parse(fileContent, {
                columns: true,
                skip_empty_lines: true,
                trim: true,
                relax_quotes: true,
                relax_column_count: true,
                quote: '"',
                delimiter: ',',
            }, (err, data: VideoRecord[]) => {
                if (err) reject(err);
                else resolve(data);
            });
        });
    } catch (error) {
        if (error.code === 'ENOENT') {
            console.log('No existing file found, starting fresh');
            return [];
        }
        throw error;
    }
}

async function main() {
    const channelId = 'UC7IcJI8PUf5Z3zKxnZvTBog'; // School of Life channel
    const filePath = 'videos.txt';

    try {
        // Read existing videos
        console.log('Reading existing videos...');
        const existingVideos = await readCsvFile(filePath);
        const existingIds = new Set(existingVideos.map(v => v.ID));
        console.log(`Found ${existingVideos.length} existing videos`);
        
        // Get new videos
        console.log('Checking for new videos...');
        const newVideos = await getNewVideos(channelId, existingIds);
        console.log(`Found ${newVideos.length} new videos`);

        // Update metadata for existing videos
        console.log('Updating metadata for existing videos...');
        const updatedExistingVideos = await updateExistingVideos(existingVideos);

        // Combine and sort all videos by publish date
        const allVideos = [...updatedExistingVideos, ...newVideos]
            .sort((a, b) => new Date(b['Published At']).getTime() - new Date(a['Published At']).getTime());

        // Process videos to convert quoted text in titles to single quotes
        const processedVideos = allVideos.map(video => ({
            ...video,
            Title: he.decode(video.Title) //decode the html entries to aviod codes!
            .replace(/[\u2018\u2019]/g, "'")  // Convert all types of single quotes to straight single quote
            .replace(/"([^"]+)"/g, "'$1'")  // Then handle double quoted text
        }));

        // Save to file
        const csvString = stringify(processedVideos, { 
            header: true,
            columns: ['ID', 'Title', 'Published At', 'Duration', 'ViewCount', 'LikeCount'],
            quoted_string: true
        });
        await fs.writeFile(filePath, csvString);

        console.log(`✅ Saved ${processedVideos.length} videos to ${filePath}`);
        console.log(`Added ${newVideos.length} new videos`);
        console.log(`Updated metadata for ${existingVideos.length} existing videos`);
        
        // Calculate approximate API units used
        const searchCalls = Math.ceil(newVideos.length / 50);
        const videoCalls = Math.ceil((newVideos.length + existingVideos.length) / 50);
        console.log(`Approximate API units used: ${(searchCalls * 100) + videoCalls}`);

    } catch (error) {
        console.error('Error:', error instanceof Error ? error.message : error);
    }
}

main();