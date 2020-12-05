import os
import sys
import argparse
import asyncio
import concurrent.futures
from functools import partial
from pathlib import Path
from tqdm import tqdm
import cv2
VIDEO_SUFFIX = ['.mp4','.3gp', '.avi']
class VideoFrame:
    def __init__(self, video_path):
        self.video_path = Path(video_path)
        self.video = cv2.VideoCapture(str(video_path))
        self.frame_num = self.video.get(cv2.CAP_PROP_FRAME_COUNT)
    def _get_pos_frame(self):
        return self.video.get(cv2.CAP_PROP_POS_FRAMES)
    def __iter__(self):
        return self
    def __next__(self):
        (ret, frame) = self.video.read()
        pos_frame = self._get_pos_frame()
        if ret:
            return (self.video_path, frame, pos_frame)
        if pos_frame == self.frame_num:
            raise StopIteration
async def make_image(video_dir, frame, frame_num):
    video_dir = Path(video_dir)
    image_path = video_dir.parent/video_dir.stem/f"{video_dir.stem}_{int(frame_num)}.jpg"
    loop = asyncio.get_running_loop()
    imwrite_func = partial(cv2.imwrite, str(image_path), frame)
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool,imwrite_func)
async def one_video(video_path, stride):
    await asyncio.wait(
        [make_image(video_path, frame, pos_frame) 
        for (video_path, frame, pos_frame)
        in VideoFrame(video_path) if (pos_frame % stride)==0])
async def many_videos(video_paths, stride):
    for video in tqdm(video_paths):
        try:
            await one_video(video, stride)
        except:
            print(f"Please Check this file {video}")
            continue
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path",help="Path of file or folder",
                        type=str)
    parser.add_argument("stride",help="Stride Frames",
                        type=int,)
    loop = asyncio.get_event_loop()
    args = parser.parse_args()
    if Path(args.path).is_dir():
        videos = [video for video in Path(args.path).glob('*') 
                    if video.suffix in VIDEO_SUFFIX]
        for video in videos:
            video.with_suffix('').mkdir(exist_ok=True)
        loop.run_until_complete(many_videos(videos,args.stride))
    else:
        Path(args.path).with_suffix('').mkdir(exist_ok=True)
        loop.run_until_complete(one_video(args.path,args.stride))